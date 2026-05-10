# baikal_seedlink_collector.py
import threading
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List

logger = logging.getLogger("BaikalSeedLink")
logger.setLevel(logging.INFO)

class BaikalSeedLinkCollector:
    """
    Сборщик данных с датчика Байкал-8 через протокол SeedLink.
    Буферизует данные и записывает в БД каждые 5 секунд.
    """
    
    def __init__(
        self,
        db_session_factory,
        chunk_model_class,
        sensor_id: int = 7,
        address: str = "192.168.1.10:18000",
        select: str = "NT_B8:CH3",
        flush_interval: float = 5.0
    ):
        self.db_session_factory = db_session_factory
        self.ChunkModel = chunk_model_class
        self.sensor_id = sensor_id
        self.address = address
        self.select = select
        self.flush_interval = flush_interval

        self.buffer: List[float] = []
        self.buffer_lock = threading.Lock()
        self.chunk_start_time: datetime = None
        self.sampling_rate: float = None
        self._packets_received = 0

        self._client_thread = None
        self._flush_thread = None
        self._running = False

    def packet_handler(self, count, slpack):
        """Обработчик входящих пакетов от ObsPy"""
        try:
            from obspy.clients.seedlink.slpacket import SLPacket
            
            # Совместимость с разными версиями ObsPy
            trace_type = getattr(SLPacket, 'TYPE_TRACE', 
                            getattr(SLPacket, 'SL_TRACE', 
                            getattr(SLPacket, 'TRACE', 1000)))
            
            if slpack.get_type() != trace_type:
                return False
            
            trace = slpack.get_trace()
            data = trace.data.tolist()
            rate = trace.stats.sampling_rate
            
            self._packets_received += 1
            
            # Отладочный вывод каждые 50 пакетов
            if self._packets_received % 50 == 0:
                logger.debug(f"📦 Пакет #{self._packets_received}: {len(data)} отсчётов, rate={rate} Гц")
            
            # === ВАЖНО: НЕ ВЫЗЫВАЕМ _flush_to_db() ВНУТРИ with buffer_lock ===
            with self.buffer_lock:
                if self.chunk_start_time is None:
                    self.chunk_start_time = datetime.now(timezone.utc)
                    self.sampling_rate = rate
                
                self.buffer.extend(data)
                
                # Проверяем, набрали ли ~5 секунд данных
                target = int(rate * self.flush_interval)
                if len(self.buffer) >= target:
                    # === Копируем данные и освобождаем лок ===
                    chunk_data = self.buffer[:target]
                    self.buffer = self.buffer[target:]
                    start = self.chunk_start_time
                    end = start + timedelta(seconds=self.flush_interval)
                    self.chunk_start_time = datetime.now(timezone.utc)
                    rate = self.sampling_rate
                    # === Лок освобождён, можно вызывать _flush_to_db() ===
            
            # === ВЫЗОВ ВНЕ БЛОКИРОВКИ ===
            if 'chunk_data' in locals() and chunk_data:
                self._flush_to_db_impl(chunk_data, start, end, rate)
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Ошибка в packet_handler: {e}", exc_info=True)
            return False

    def _flush_to_db_impl(self, chunk_data, start, end, rate):
        """Реальная запись в БД (вызывается вне блокировки)"""
        try:

            # Локальный импорт модели (разрывает циклические зависимости)
            with self.db_session_factory() as db:
                chunk = self.ChunkModel(
                    sensor_id=self.sensor_id,
                    chunk_start=start,
                    chunk_end=end,
                    sampling_rate=rate,
                    raw_values=chunk_data
                )
                db.add(chunk)
                db.commit()
            
        except Exception as e:
            logger.error(f"❌ ОШИБКА записи в БД: {e}", exc_info=True)
            import traceback
            logger.error(traceback.format_exc())

    def _flush_to_db(self):
        """Таймер-страховка: сбрасывает буфер в БД каждые 5 сек"""
        try:
            # === Копируем данные внутри блокировки ===
            with self.buffer_lock:
                if not self.buffer or self.sampling_rate is None:
                    logger.debug("⚠️ _flush_to_db: буфер пуст или нет sampling_rate")
                    return
                
                target = int(self.sampling_rate * self.flush_interval)
                chunk_data = self.buffer[:target]
                self.buffer = self.buffer[target:]
                
                start = self.chunk_start_time
                end = start + timedelta(seconds=self.flush_interval)
                self.chunk_start_time = datetime.now(timezone.utc)
                rate = self.sampling_rate

            if not chunk_data:
                logger.debug("⚠️ _flush_to_db: chunk_data пуст после среза")
                return

            # === Вызываем реализацию вне блокировки ===
            self._flush_to_db_impl(chunk_data, start, end, rate)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в _flush_to_db: {e}", exc_info=True)

    def _run_client(self):
        """Запуск ObsPy SeedLink клиента в отдельном потоке"""
        from obspy.clients.seedlink.slclient import SLClient
        
        class MySLClient(SLClient):
            def packet_handler(self, count, slpack):
                return self.collector.packet_handler(count, slpack)

        client = MySLClient()
        client.collector = self
        client.slconn.set_sl_address(self.address)
        client.multiselect = self.select

        logger.info(f"🔌 SeedLink client: connecting to {self.address}")
        
        while self._running:
            try:
                client.initialize()
                logger.info("🟢 SeedLink initialized, starting run()...")
                client.run()  # Блокирующий вызов
            except Exception as e:
                logger.warning(f"⚠️ SeedLink error: {e}. Reconnect in 3s...")
                time.sleep(3)
            finally:
                try:
                    client.terminate()
                except:
                    pass

    def _flush_loop(self):
        """Фоновый таймер для периодического сброса буфера (страховка)"""
        while self._running:
            time.sleep(self.flush_interval)
            if not self._running:
                break
            # Страховка: если в буфере что-то есть — сбрасываем
            with self.buffer_lock:
                if self.buffer and self.sampling_rate:
                    logger.info(f"⏰ Timer flush: buffer has {len(self.buffer)} points")
                    # Копируем и вызываем вне блокировки
                    target = int(self.sampling_rate * self.flush_interval)
                    chunk_data = self.buffer[:target]
                    self.buffer = self.buffer[target:]
                    start = self.chunk_start_time
                    end = start + timedelta(seconds=self.flush_interval)
                    self.chunk_start_time = datetime.now(timezone.utc)
                    rate = self.sampling_rate
            
            if 'chunk_data' in locals() and chunk_data:
                self._flush_to_db_impl(chunk_data, start, end, rate)

    def start(self):
        """Запускает сборщик в фоновых потоках"""
        if self._running:
            return
        self._running = True
        
        logger.info("🚀 Starting Baikal SeedLink Collector...")
        
        # Поток записи в БД (таймер-страховка)
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        
        # Поток SeedLink клиента
        self._client_thread = threading.Thread(target=self._run_client, daemon=True)
        self._client_thread.start()

    def stop(self):
        """Останавливает сборщик и сохраняет остаток буфера"""
        self._running = False
        logger.info("⏹️ Stopping collector...")
        
        # Финальный сброс буфера
        with self.buffer_lock:
            if self.buffer and self.sampling_rate:
                logger.info(f"🔄 Final flush: {len(self.buffer)} points remaining")
                target = int(self.sampling_rate * self.flush_interval)
                chunk_data = self.buffer[:target]
                start = self.chunk_start_time
                end = start + timedelta(seconds=self.flush_interval)
                rate = self.sampling_rate
            else:
                chunk_data = None
        
        if chunk_data:
            self._flush_to_db_impl(chunk_data, start, end, rate)
        
        if self._client_thread:
            self._client_thread.join(timeout=5)
        if self._flush_thread:
            self._flush_thread.join(timeout=2)
        logger.info("✅ Collector stopped")
