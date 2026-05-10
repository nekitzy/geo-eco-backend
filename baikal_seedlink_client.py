#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
baikal_usb_stream_client.py
Мониторинг папки, куда baikal-control сохраняет USB-поток
"""
import asyncio
import logging
import numpy as np
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from obspy import read, UTCDateTime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship

# ============================================================================
# === КОНФИГУРАЦИЯ (ИЗМЕНИТЕ ПУТЬ НА СВОЙ!) ===
# ============================================================================
# Папка, куда baikal-control сохраняет поток (Stream → Settings → Directory)
STREAM_OUTPUT_DIR = Path(r"C:\baykal-control")
ARCHIVE_DIR = STREAM_OUTPUT_DIR / "archive"
PROCESSED_DIR = STREAM_OUTPUT_DIR / "processed"

POLL_INTERVAL = 3          # Секунды между проверками
MIN_FILE_AGE = 8           # Игнорировать файлы, записанные менее N сек назад
FILE_EXTENSIONS = {".mseed", ".ms", ".miniseed", ".xx", ".seed"}

DB_CONFIG = {
    "dbname": "noiselevel_utf8",
    "user": "postgres",
    "password": "Master200455",
    "host": "localhost",
    "port": "5432",
    "client_encoding": "UTF8"
}
DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    f"?client_encoding=UTF8"
)

# ============================================================================
# === ЛОГИРОВАНИЕ ===
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('baikal_usb_stream.log', encoding='utf-8')
    ]
)
logger = logging.getLogger("BaikalUSBStream")

# ============================================================================
# === БД ===
# ============================================================================
Base = declarative_base()

class Sensor(Base):
    __tablename__ = "sensors"
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    address = Column(String(255))
    sensor_type = Column(String(50))
    status = Column(String(20), default="active")
    created_at = Column(DateTime, default=datetime.utcnow)
    measurements = relationship("Measurement", back_populates="sensor", cascade="all, delete-orphan")

class Measurement(Base):
    __tablename__ = "measurements"
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"), nullable=False)
    noise_level = Column(Float)
    measured_at = Column(DateTime, default=datetime.utcnow, index=True)
    is_anomaly = Column(Boolean, default=False)
    anomaly_type = Column(String(50))
    sensor = relationship("Sensor", back_populates="measurements")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ============================================================================
# === КЛИЕНТ ===
# ============================================================================
class BaikalStreamClient:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.archive_dir = ARCHIVE_DIR
        self.processed_files = set()
        self.sensor_id = "baikal_usb_stream"
        self._ensure_dirs()

    def _ensure_dirs(self):
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def _is_file_ready(self, filepath: Path) -> bool:
        try:
            mtime = filepath.stat().st_mtime
            return (time.time() - mtime) > MIN_FILE_AGE
        except Exception:
            return False

    def scan_new_files(self):
        if not self.output_dir.exists():
            logger.warning(f"🔌 Папка не найдена: {self.output_dir}")
            return []

        ready = []
        for f in self.output_dir.iterdir():
            if (f.is_file() and 
                f.suffix.lower() in FILE_EXTENSIONS and 
                f.name not in self.processed_files and 
                self._is_file_ready(f)):
                ready.append(f)
        return sorted(ready, key=lambda x: x.stat().st_mtime)

    def calculate_noise_level(self, filepath: Path):
        try:
            # ObsPy поддерживает .mseed нативно. .xx может потребовать конвертации.
            st = read(str(filepath))
            if not st: return None, None
            
            trace = st[0]
            data = trace.data.astype(float)
            if len(data) == 0: return None, None

            rms = np.sqrt(np.mean(data**2))
            noise_db = 20 * np.log10(rms + 1e-10) + 40
            noise_db = round(min(90, max(30, noise_db)), 1)
            timestamp = trace.stats.endtime.datetime
            return noise_db, timestamp
        except Exception as e:
            if ".xx" in str(filepath).lower():
                logger.warning(f"⚠️ Формат .xx не поддерживается ObsPy напрямую. Настройте Stream в MiniSEED.")
            else:
                logger.error(f"❌ Ошибка чтения {filepath.name}: {e}")
            return None, None

    def ensure_sensor_in_db(self, db_session):
        sensor = db_session.query(Sensor).filter(Sensor.sensor_id == self.sensor_id).first()
        if not sensor:
            sensor = Sensor(
                sensor_id=self.sensor_id, name="Байкал-8 (USB-Stream)",
                description="Поток через baikal-control (COM-порт)",
                latitude=54.846667, longitude=83.106667,
                address="Локальный ПК", sensor_type="vibration", status="active"
            )
            db_session.add(sensor)
            db_session.commit()
        return sensor.id

    def save_to_db(self, noise_level, timestamp):
        db = SessionLocal()
        try:
            sensor_id = self.ensure_sensor_in_db(db)
            is_anomaly = noise_level >= 70
            anomaly_type = "danger_high_noise" if noise_level >= 70 else ("warning_high_noise" if noise_level >= 55 else None)
            
            measurement = Measurement(
                sensor_id=sensor_id, noise_level=noise_level,
                measured_at=timestamp, is_anomaly=is_anomaly, anomaly_type=anomaly_type
            )
            db.add(measurement)
            db.commit()
            logger.info(f"💾 БД: Шум={noise_level} дБ | {timestamp}")
        except Exception as e:
            logger.error(f"❌ Ошибка БД: {e}")
            db.rollback()
        finally:
            db.close()

    def archive_file(self, filepath: Path):
        try:
            dest = self.archive_dir / filepath.name
            shutil.move(str(filepath), str(dest))
            self.processed_files.add(filepath.name)
        except Exception as e:
            logger.error(f"⚠️ Ошибка архивации {filepath.name}: {e}")

    async def run(self):
        logger.info("="*60)
        logger.info("🔌 ЗАПУСК USB-STREAM КЛИЕНТА")
        logger.info(f"📁 Мониторинг: {self.output_dir}")
        logger.info("="*60)

        if not self.output_dir.exists():
            logger.error(f"❌ Папка не существует. Создайте: {self.output_dir}")
            logger.info("💡 Укажите этот путь в baikal-control → Stream → Settings → Directory")
            return

        logger.info("🔄 Ожидание новых файлов... (Ctrl+C для выхода)")
        while True:
            try:
                new_files = self.scan_new_files()
                if new_files:
                    logger.info(f"📥 Найдено {len(new_files)} новых файлов")
                    for f in new_files:
                        noise, ts = self.calculate_noise_level(f)
                        if noise is not None:
                            self.save_to_db(noise, ts)
                        self.archive_file(f)
                else:
                    logger.debug("📭 Нет новых готовых файлов")

                await asyncio.sleep(POLL_INTERVAL)
            except KeyboardInterrupt:
                logger.info("⏹ Остановка пользователем")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка цикла: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    try: Base.metadata.create_all(bind=engine)
    except Exception as e: logger.error(f"❌ БД: {e}"); sys.exit(1)

    client = BaikalStreamClient(output_dir=STREAM_OUTPUT_DIR)
    asyncio.run(client.run())
