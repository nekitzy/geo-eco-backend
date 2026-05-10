import os
import asyncio
import numpy as np
from datetime import datetime, timedelta
from obspy import Trace, Stream
from obspy.core.utcdatetime import UTCDateTime
from config import SENSOR_FOLDERS

SENSOR_BASE_NOISE = {
    "sensor_1": 55,  
    "sensor_2": 52,  
    "sensor_3": 58,  
    "sensor_4": 54,  
}

NOISE_LIMITS = {
    "night": {"min": 35, "max": 60, "mean": 48},   
    "day": {"min": 40, "max": 70, "mean": 58},     
}

ANOMALY_CHANCE = {
    "night": 0.05,  
    "day": 0.08,   
}

def get_time_period():
    """Определение времени суток"""
    hour = datetime.now().hour
    if 22 <= hour or hour < 6:
        return "night"
    else:
        return "day"

def get_realistic_noise_level(base_noise: float, sensor_id: str) -> float:
    time_period = get_time_period()
    limits = NOISE_LIMITS[time_period]
    
    noise_level = np.random.normal(base_noise, 3)
    
    is_anomaly = np.random.random() < ANOMALY_CHANCE[time_period]
    
    if is_anomaly:
        anomaly_boost = np.random.uniform(5, 15)
        noise_level = limits["max"] + anomaly_boost
    else:
        noise_level = max(limits["min"], min(limits["max"], noise_level))
    
    noise_level += np.random.uniform(-2, 2)
    
    if time_period == "night":
        noise_level = max(35, min(65, noise_level))  
    else:
        noise_level = max(40, min(75, noise_level))  
    
    return round(noise_level, 1)

def create_mseed_file(sensor_id: str, noise_level: float, duration_sec: float = 1.0, sampling_rate: int = 500):
    folder = SENSOR_FOLDERS.get(sensor_id)
    if not folder:
        return None
    
    os.makedirs(folder, exist_ok=True)
    
    npts = int(duration_sec * sampling_rate)
    t = np.linspace(0, duration_sec, npts)
    
    amplitude = (noise_level - 30) / 70 
    
    signal = np.sin(2 * np.pi * 5 * t) * amplitude
    noise = np.random.normal(0, 0.1, npts) * amplitude
    data = signal + noise
    
    if np.random.random() > 0.8:
        spike_pos = np.random.randint(0, npts - 10)
        data[spike_pos:spike_pos+10] += np.random.normal(0, 0.5, 10) * amplitude
    
    trace = Trace(
        data=data.astype(np.float32),
        header={
            'network': 'NT',
            'station': sensor_id,
            'location': '00',
            'channel': 'CH0',
            'starttime': UTCDateTime.now() - duration_sec,
            'sampling_rate': sampling_rate,
            'npts': npts,
            'comment': f'noise_level:{noise_level:.1f}'
        }
    )
    
    stream = Stream([trace])
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
    filename = os.path.join(folder, f"{sensor_id}_{timestamp}.mseed")
    stream.write(filename, format='MSEED')
    
    return filename

async def generate_for_sensor(sensor_id: str, base_noise: float):
    """Генерация файлов для одного датчика"""
    while True:
        try:
            noise_level = get_realistic_noise_level(base_noise, sensor_id)
            
            filename = create_mseed_file(sensor_id, noise_level)
            
            if filename:
                time_period = get_time_period()
            
            await asyncio.sleep(3)
            
        except Exception as e:
            print(f"Ошибка генерации для {sensor_id}: {e}")
            await asyncio.sleep(3)

async def start_all_generators():
    """Запуск всех генераторов"""
    tasks = [
        asyncio.create_task(generate_for_sensor(sid, noise))
        for sid, noise in SENSOR_BASE_NOISE.items()
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(start_all_generators())
