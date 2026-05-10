# baikal_vibration_monitor.py
import os
import glob
import asyncio
import logging
import numpy as np
from datetime import datetime
from typing import Set, Optional
from obspy import read
import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG, BAYKAL_SENSOR_DB_ID, BAYKAL_ARCHIVE_DIR, ANOMALY_THRESHOLDS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BaikalVibrationMonitor")


VIBRATION_THRESHOLD = ANOMALY_THRESHOLDS["vibration"]["danger"]

class BaikalVibrationMonitor:
    def __init__(self, archive_dir: str = BAYKAL_ARCHIVE_DIR):
        self.archive_dir = archive_dir
        self.processed_files: Set[str] = set()
        self.websockets = []
        self.db_connection = None
        os.makedirs(self.archive_dir, exist_ok=True)

    async def connect(self, websocket):
        await websocket.accept()
        self.websockets.append(websocket)

    def disconnect(self, websocket):
        if websocket in self.websockets:
            self.websockets.remove(websocket)

    async def broadcast(self,  dict):
        for ws in self.websockets[:]:
            try:
                await ws.send_json(data)
            except:
                self.websockets.remove(ws)

    def get_new_files(self):
        if not os.path.exists(self.archive_dir): return []
        pattern = os.path.join(self.archive_dir, "*.mseed")
        return [f for f in glob.glob(pattern) if os.path.basename(f) not in self.processed_files]

    def calculate_vibration(self, filepath: str) -> Optional[float]:
        try:
            st = read(filepath)
            if not st: return None
            data = st[0].data.astype(float)
            if len(data) == 0: return None
            return float(np.sqrt(np.mean(data**2)))  # RMS
        except Exception as e:
            logger.error(f"Ошибка парсинга {filepath}: {e}")
            return None

    def save_to_db(self, vibration_level: float, timestamp: datetime) -> bool:
        try:
            if not self.db_connection or self.db_connection.closed:
                self.db_connection = psycopg2.connect(**DB_CONFIG)
            cur = self.db_connection.cursor()
            is_anomaly = vibration_level > VIBRATION_THRESHOLD
            cur.execute("""
                INSERT INTO measurements (sensor_id, vibration_level, measured_at, is_anomaly, anomaly_type)
                VALUES (%s, %s, %s, %s, %s)
            """, (BAYKAL_SENSOR_DB_ID, round(vibration_level, 4), timestamp, is_anomaly, "high_vibration" if is_anomaly else None))
            self.db_connection.commit()
            cur.close()
            return True
        except Exception as e:
            logger.error(f"Ошибка БД: {e}")
            if self.db_connection: self.db_connection.rollback()
            return False

    async def check_for_new_files(self):
        files = self.get_new_files()
        if not files: return
        
        logger.info(f"📥 Найдено {len(files)} новых файлов")
        for f in files:
            vib = self.calculate_vibration(f)
            if vib is not None:
                ts = datetime.fromtimestamp(os.path.getmtime(f))
                self.save_to_db(vib, ts)
                await self.broadcast({
                    'type': 'vibration_update',
                    'sensor_id': BAYKAL_SENSOR_DB_ID,
                    'vibration_level': round(vib, 4),
                    'measured_at': ts.isoformat(),
                    'filename': os.path.basename(f),
                    'is_anomaly': vib > VIBRATION_THRESHOLD
                })
                logger.info(f"💾 Вибрация: {vib:.4f} | Аномалия: {'🚨' if vib > VIBRATION_THRESHOLD else '✅'}")
            self.processed_files.add(os.path.basename(f))

    async def start_monitoring(self, interval: float = 5.0):
        os.makedirs(self.archive_dir, exist_ok=True)
        logger.info(f"🔄 Запуск мониторинга вибрации: {self.archive_dir}")
        while True:
            try:
                await self.check_for_new_files()
            except Exception as e:
                logger.error(f"Ошибка мониторинга: {e}")
            await asyncio.sleep(interval)

    def get_stats(self, hours: int = 24) -> dict:
        try:
            if not self.db_connection or self.db_connection.closed:
                self.db_connection = psycopg2.connect(**DB_CONFIG)
            cur = self.db_connection.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE is_anomaly = TRUE) as total,
                    COUNT(*) FILTER (WHERE is_anomaly = TRUE AND EXTRACT(HOUR FROM measured_at) >= 6 AND EXTRACT(HOUR FROM measured_at) < 23) as day_count,
                    COUNT(*) FILTER (WHERE is_anomaly = TRUE AND (EXTRACT(HOUR FROM measured_at) < 6 OR EXTRACT(HOUR FROM measured_at) >= 23)) as night_count,
                    COALESCE(AVG(vibration_level), 0) as avg_vibration
                FROM measurements 
                WHERE sensor_id = %s
            """, (BAYKAL_SENSOR_DB_ID,))
            res = cur.fetchone()
            cur.close()
            return {
                "total": res['total'] or 0,
                "day": res['day_count'] or 0,
                "night": res['night_count'] or 0,
                "avg_vibration": round(res['avg_vibration'], 2) if res['avg_vibration'] else 0
            }
        except Exception as e:
            logger.error(f"Ошибка статистики: {e}")
            return {"total": 0, "day": 0, "night": 0, "avg_vibration": 0}

BAIKAL_VIBRATION_MONITOR = BaikalVibrationMonitor()
