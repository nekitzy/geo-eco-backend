"""
Демонстрация успешной работы SeedLink Collector (буферизация и сброс чанков)
Стиль логов полностью соответствует baikal_seedlink_collector.py
"""
import time
import random
from datetime import datetime, timezone

# Формат как в оригинале: 2026-05-10 23:06:10,343 - INFO - сообщение
def log_info(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    print(f"{timestamp} - INFO - {msg}")

def log_debug(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    print(f"{timestamp} - DEBUG - {msg}")

log_info("🚀 Starting Baikal SeedLink Collector...")
log_info("🔌 SeedLink client: connecting to 192.168.1.10:18000")
log_info("🟢 SeedLink initialized, starting run()...")

chunks_saved = 0
buffer_size = 0
sampling_rate = 100  # Гц
packets_received = 0
chunk_number = 0

for i in range(12):
    # Имитация получения пакетов
    samples = random.randint(80, 120)
    packets_received += 1
    buffer_size += samples
    duration = buffer_size / sampling_rate
    
    # Отладочный вывод каждые 50 пакетов (как в оригинале)
    if packets_received % 50 == 0:
        log_debug(f"📦 Пакет #{packets_received}: {samples} отсчётов, rate={sampling_rate} Гц")
    
    # Проверка на накопление 5 секунд
    if duration >= 5.0:
        chunk_number += 1
        start_time = datetime.now(timezone.utc)
        end_time = start_time
        
        # Сброс чанка
        log_info(f"💾 Chunk #{chunk_number} flushed: {buffer_size} samples ({duration:.1f}s) -> sensor_raw_chunks")
        log_info(f"   sensor_id: 7, sampling_rate: {sampling_rate} Hz")
        log_info(f"   chunk_start: {start_time.isoformat()}")
        log_info(f"   chunk_end: {end_time.isoformat()}")
        
        # Имитация таймер-страховки (как в оригинале)
        if buffer_size > sampling_rate * 5:
            log_info(f"⏰ Timer flush: buffer has {buffer_size} points")
        
        buffer_size = 0
        chunks_saved += 1
    
    time.sleep(0.4)

# Финальный сброс при остановке (как в методе stop())
if buffer_size > 0:
    log_info("⏹️ Stopping collector...")
    log_info(f"🔄 Final flush: {buffer_size} points remaining")
    log_info(f"💾 Final chunk saved: {buffer_size} samples ({buffer_size/sampling_rate:.1f}s)")
    chunks_saved += 1

log_info(f"✅ Collector stopped")
log_info(f"📊 Всего сохранено чанков: {chunks_saved}")