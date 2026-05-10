import os
import asyncio
import glob
from datetime import datetime
from typing import Dict, Set, Optional
from obspy import read
from fastapi import WebSocket
from mseed_generator import SENSOR_FOLDERS
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from config import SENSOR_FOLDERS, DB_CONFIG, SENSOR_ID_MAPPING, ANOMALY_THRESHOLDS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MSeedMonitor:
    """Монитор папок с miniSEED файлами с сохранением в БД"""
    
    def __init__(self, folders: Dict[str, str]):
        self.folders = {k.strip(): v.strip() for k, v in folders.items()}
        self.processed_files: Dict[str, Set[str]] = {sid: set() for sid in self.folders}
        self.websockets: list = []
        self.db_connection = None
        self.db_cursor = None

    def get_db_connection(self):
        """Подключение к БД"""
        try:
            if self.db_connection is None or self.db_connection.closed:
                self.db_connection = psycopg2.connect(
                    dbname=DB_CONFIG['dbname'],
                    user=DB_CONFIG['user'],
                    password=DB_CONFIG['password'],
                    host=DB_CONFIG['host'],
                    port=DB_CONFIG['port'],
                    options='-c client_encoding=UTF8'
                )
                self.db_cursor = self.db_connection.cursor(cursor_factory=RealDictCursor)
                self.db_connection.commit()
                logger.info("Подключение к БД установлено")
            return self.db_connection
        except psycopg2.OperationalError as e:
            logger.error(f"Ошибка подключения к PostgreSQL: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            return None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.websockets.append(websocket)
        logger.info(f"WebSocket подключен. Всего: {len(self.websockets)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.websockets:
            self.websockets.remove(websocket)
            logger.info(f"WebSocket отключен. Всего: {len(self.websockets)}")

    async def broadcast(self, data: dict):
        disconnected = []
        for ws in self.websockets:
            try:
                await ws.send_json(data)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            self.disconnect(ws)

    def get_new_files(self, sensor_id: str) -> list:
        folder = self.folders.get(sensor_id)
        if not folder or not os.path.exists(folder):
            return []
        pattern = os.path.join(folder, "*.mseed")
        all_files = glob.glob(pattern)
        new_files = [
            f for f in all_files
            if os.path.basename(f) not in self.processed_files[sensor_id]
        ]
        return new_files

    def extract_noise_level(self, filepath: str) -> Optional[float]:
        try:
            stream = read(filepath)
            if not stream:
                return None
            trace = stream[0]
            comment = getattr(trace.stats, 'comment', '')
            if comment and 'noise_level:' in str(comment):
                try:
                    noise_str = str(comment).split('noise_level:')[1].strip().split()[0]
                    noise_val = float(noise_str)
                    logger.info(f"{os.path.basename(filepath)}: comment = {noise_val} dB")
                    return round(noise_val, 1)
                except (IndexError, ValueError) as e:
                    logger.warning(f"Не распарсен comment: {e}")

            data = trace.data
            if len(data) == 0:
                return None
            rms = np.sqrt(np.mean(data**2))
            scaled_rms = rms * 150
            noise_db = 20 * np.log10(scaled_rms + 1e-10) + 27
            random_variation = np.random.uniform(-30, 10)
            noise_db += random_variation
            peak = np.max(np.abs(data))
            if rms > 0:
                peak_factor = peak / rms
                if peak_factor > 3:
                    noise_db += np.random.uniform(2, 6)  
                elif peak_factor > 2:
                    noise_db += np.random.uniform(0, 3)  
            noise_db = max(35, min(95, noise_db))
            logger.info(f"{os.path.basename(filepath)}: RMS={rms:.4f} → {noise_db:.1f} dB")
            return round(noise_db, 1)
        except Exception as e:
            logger.error(f"Ошибка чтения {filepath}: {e}")
            return None

    def detect_anomaly(self, noise_level: float) -> tuple:
        """Определение аномалии на основе порогов"""
        thresholds = ANOMALY_THRESHOLDS.get("noise_level", {})
        
        if noise_level >= thresholds.get("danger", 70.0):
            return True, "danger_high_noise"
        elif noise_level >= thresholds.get("warning", 55.0):
            return False, "warning_high_noise"
        elif noise_level < thresholds.get("min", 30.0):
            return True, "low_noise"
        
        return False, None

    def save_to_database(self, sensor_id: str, noise_level: float) -> bool:
        """Сохранение измерения в базу данных"""
        try:
            conn = self.get_db_connection()
            if not conn:
                return False

            db_sensor_id = SENSOR_ID_MAPPING.get(sensor_id.strip())
            if not db_sensor_id:
                logger.warning(f"Не найден маппинг для {sensor_id}")
                return False

            is_anomaly, anomaly_type = self.detect_anomaly(noise_level)

            temperature = round(np.random.normal(-5, 5), 2)
            humidity = round(np.random.normal(65, 15), 2)
            air_quality_index = round(np.random.normal(50, 20), 0)

            self.db_cursor.execute("""
                INSERT INTO measurements (
                    sensor_id, noise_level, temperature, humidity,
                    air_quality_index, vibration_level, measured_at,
                    is_anomaly, anomaly_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                db_sensor_id,
                float(noise_level),
                float(temperature),
                float(humidity),
                int(air_quality_index),
                None,
                datetime.now(),
                is_anomaly,
                anomaly_type
            ))
            conn.commit()
            
            if is_anomaly:
                logger.warning(f"АНОМАЛИЯ {sensor_id}: шум={noise_level} dB, тип={anomaly_type}")
            else:
                logger.info(f"{sensor_id}: Сохранено - шум={noise_level} dB")
            return True

        except psycopg2.Error as e:
            logger.error(f"Ошибка PostgreSQL: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            return False
        except Exception as e:
            logger.error(f"Ошибка сохранения: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            return False

    async def check_for_new_files(self):
        """Проверка новых файлов и обработка"""
        for sensor_id in self.folders:
            new_files = self.get_new_files(sensor_id)
            if new_files:
                latest_file = max(new_files, key=os.path.getmtime)
                filename = os.path.basename(latest_file)
                noise_level = self.extract_noise_level(latest_file)

                if noise_level is not None:
                    self.save_to_database(sensor_id, noise_level)

                    update = {
                        'type': 'mseed_update',
                        'sensor_id': sensor_id,
                        'filename': filename,
                        'noise_level': float(noise_level),
                        'timestamp': datetime.now().isoformat()
                    }
                    await self.broadcast(update)
                    logger.info(f"Отправлено: {sensor_id} = {noise_level} dB")

                for f in new_files:
                    self.processed_files[sensor_id].add(os.path.basename(f))

                self._cleanup_old_files(sensor_id)

    def _cleanup_old_files(self, sensor_id: str, keep_last: int = 10):
        folder = self.folders.get(sensor_id)
        if not folder:
            return
        pattern = os.path.join(folder, "*.mseed")
        files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
        for old_file in files[keep_last:]:
            try:
                os.remove(old_file)
                self.processed_files[sensor_id].discard(os.path.basename(old_file))
            except Exception:
                pass

    async def start_monitoring(self, interval: float = 2.0):
        logger.info(f"Запуск мониторинга (интервал: {interval}с)")
        while True:
            try:
                await self.check_for_new_files()
            except Exception as e:
                logger.error(f"Ошибка мониторинга: {e}")
            await asyncio.sleep(interval)

    def close(self):
        try:
            if self.db_cursor:
                self.db_cursor.close()
            if self.db_connection and not self.db_connection.closed:
                self.db_connection.close()
                logger.info("Подключение к БД закрыто")
        except Exception:
            pass

    def get_anomaly_statistics(self, hours: int = 24) -> dict:
        """Получение статистики аномалий"""
        try:
            conn = self.get_db_connection()
            if not conn:
                return {"error": "No database connection"}
            
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT COUNT(*) as total_anomalies
                FROM measurements
                WHERE is_anomaly = TRUE
                AND measured_at >= NOW() - INTERVAL '%s hours'
            """, (hours,))
            total = cursor.fetchone()['total_anomalies']
            
            cursor.execute("""
                SELECT anomaly_type, COUNT(*) as count
                FROM measurements
                WHERE is_anomaly = TRUE
                AND measured_at >= NOW() - INTERVAL '%s hours'
                GROUP BY anomaly_type
            """, (hours,))
            by_type = {row['anomaly_type']: row['count'] for row in cursor.fetchall()}
            
            cursor.execute("""
                SELECT m.sensor_id, s.name, COUNT(*) as count
                FROM measurements m
                JOIN sensors s ON s.id = m.sensor_id
                WHERE m.is_anomaly = TRUE
                AND m.measured_at >= NOW() - INTERVAL '%s hours'
                GROUP BY m.sensor_id, s.name
            """, (hours,))
            by_sensor = [dict(row) for row in cursor.fetchall()]
            
            cursor.close()
            
            return {
                "period_hours": hours,
                "total_anomalies": total,
                "by_type": by_type,
                "by_sensor": by_sensor,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {"error": str(e)}

MSEED_MONITOR = MSeedMonitor(SENSOR_FOLDERS)
