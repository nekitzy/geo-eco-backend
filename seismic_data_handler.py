import os
import glob
import asyncio
import json
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fastapi import APIRouter, WebSocket, HTTPException
from fastapi.responses import JSONResponse
from config import MINISEED_DIR

router = APIRouter()

class MiniSeedHandler:
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.stations_data: Dict[str, Dict] = {}
        self._last_processed_files: set = set()
        self._use_simulation = False
        
        try:
            from obspy import read
        except ImportError:
            self._use_simulation = True
            self._generate_simulated_data()
    
    def get_mseed_files(self) -> List[str]:
        """Получить список miniSEED файлов"""
        if self._use_simulation:
            return list(self.stations_data.keys())
        
        pattern = os.path.join(self.data_dir, "*.mseed")
        files = glob.glob(pattern)
        return files
    
    def read_station_data(self, filepath: str) -> Optional[Dict]:
        if self._use_simulation:
            return self.stations_data.get(os.path.basename(filepath).replace('.mseed', ''))
        
        try:
            from obspy import read
            stream = read(filepath)
            
            if not stream or len(stream) == 0:
                return None
            
            station_id = os.path.basename(filepath).replace('.mseed', '')
            traces_data = []
            
            for i, trace in enumerate(stream):
                try:
                    sampling_rate = getattr(trace.stats, 'sampling_rate', None)
                    if sampling_rate is None:
                        sampling_rate = getattr(trace.stats, 'delta', 1.0)
                        if sampling_rate and sampling_rate > 0:
                            sampling_rate = 1.0 / sampling_rate
                        else:
                            sampling_rate = 100.0  
                    
                    data_array = trace.data
                    if len(data_array) == 0:
                        continue
                    
                    step = max(1, len(data_array) // 500)
                    simplified_data = data_array[::step].tolist()
                    
                    starttime = getattr(trace.stats, 'starttime', None)
                    if starttime:
                        starttime = str(starttime)
                    else:
                        starttime = datetime.now().isoformat()
                    
                    traces_data.append({
                        'channel': getattr(trace.stats, 'channel', f'CH{i}'),
                        'network': getattr(trace.stats, 'network', 'NT'),
                        'station': getattr(trace.stats, 'station', station_id),
                        'location': getattr(trace.stats, 'location', '00'),
                        'starttime': starttime,
                        'endtime': str(getattr(trace.stats, 'endtime', datetime.now())),
                        'sampling_rate': float(sampling_rate),
                        'npts': len(data_array),
                        'data': simplified_data,
                        'min': float(np.min(data_array)) if len(data_array) > 0 else 0,
                        'max': float(np.max(data_array)) if len(data_array) > 0 else 0,
                        'mean': float(np.mean(data_array)) if len(data_array) > 0 else 0,
                        'std': float(np.std(data_array)) if len(data_array) > 0 else 0
                    })
                except Exception as trace_error:
                    print(f"Ошибка обработки трассы {i}: {trace_error}")
                    continue
            
            if not traces_data:
                print(f"Нет данных в файле: {filepath}")
                return None
            
            result = {
                'station_id': station_id,
                'filename': os.path.basename(filepath),
                'filepath': filepath,
                'traces': traces_data,
                'trace_count': len(traces_data),
                'processed_at': datetime.now().isoformat()
            }
            
            self.stations_data[station_id] = result
            self._last_processed_files.add(os.path.basename(filepath))
            print(f"Обработан: {filepath} ({len(traces_data)} трасс)")
            return result
            
        except Exception as e:
            print(f"Ошибка чтения {filepath}: {e}")
            print(f"   Тип ошибки: {type(e).__name__}")
            return None
    
    def _generate_simulated_data(self):
        """Генерация тестовых данных если файлы не читаются"""
        import random
        import math
        
        stations = [
            {"id": "SEISMIC_001", "name": "НГУ - Главный корпус"},
            {"id": "SEISMIC_002", "name": "Бердское шоссе"},
            {"id": "SEISMIC_003", "name": "Парк Академгородка"},
        ]
        
        for station in stations:
            traces_data = []
            for ch in ["CH0", "CH1", "CH2"]:
                npts = 500
                data = []
                for i in range(npts):
                    t = i / npts * 2 * math.pi
                    value = math.sin(t * 5) * 0.3 + random.uniform(-0.1, 0.1)
                    data.append(round(value, 4))
                
                traces_data.append({
                    'channel': ch,
                    'network': 'NT',
                    'station': station['id'],
                    'location': '00',
                    'starttime': (datetime.now() - timedelta(hours=1)).isoformat(),
                    'endtime': datetime.now().isoformat(),
                    'sampling_rate': 100,
                    'npts': npts,
                    'data': data,
                    'min': float(min(data)),
                    'max': float(max(data)),
                    'mean': float(np.mean(data)),
                    'std': float(np.std(data))
                })
            
            self.stations_data[station['id']] = {
                'station_id': station['id'],
                'filename': f"{station['id']}.mseed (симуляция)",
                'traces': traces_data,
                'trace_count': len(traces_data),
                'processed_at': datetime.now().isoformat()
            }
        
        print(f"📊 Режим симуляции: создано {len(stations)} станций")
    
    def process_new_files(self) -> Dict[str, Dict]:
        """Обработать новые miniSEED файлы"""
        files = self.get_mseed_files()
        new_data = {}
        
        for filepath in files:
            filename = os.path.basename(filepath)
            if filename not in self._last_processed_files:
                data = self.read_station_data(filepath)
                if data:
                    new_data[data['station_id']] = data
                    print(f"✅ Добавлена станция: {data['station_id']}")
        
        return new_data
    
    def get_all_stations(self) -> List[Dict]:
        """Получить список всех станций"""
        self.process_new_files()
        return [
            {
                'station_id': sid,
                'trace_count': data['trace_count'],
                'processed_at': data['processed_at'],
                'filename': data['filename']
            }
            for sid, data in self.stations_data.items()
        ]
    
    def get_station_detail(self, station_id: str) -> Optional[Dict]:
        """Получить полные данные станции"""
        self.process_new_files()
        return self.stations_data.get(station_id)

SEED_DIR = str(MINISEED_DIR)


seed_handler = MiniSeedHandler(SEED_DIR)

class SeismicWSManager:
    def __init__(self):
        self.connections: List[WebSocket] = []
    
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)
        print(f"WebSocket подключен. Всего: {len(self.connections)}")
    
    def disconnect(self, ws: WebSocket):
        if ws in self.connections:
            self.connections.remove(ws)
            print(f"WebSocket отключен. Всего: {len(self.connections)}")
    
    async def broadcast(self, data: dict):
        disconnected = []
        for ws in self.connections:
            try:
                await ws.send_json(data)
            except Exception as e:
                print(f"⚠️ Ошибка отправки: {e}")
                disconnected.append(ws)
        for ws in disconnected:
            self.disconnect(ws)

ws_manager = SeismicWSManager()


@router.get("/api/seismic/stations")
async def get_seismic_stations():
    """Список всех станций"""
    return {"stations": seed_handler.get_all_stations()}

@router.get("/api/seismic/station/{station_id}")
async def get_seismic_station(station_id: str):
    """Детальные данные станции"""
    data = seed_handler.get_station_detail(station_id)
    if not data:
        raise HTTPException(status_code=404, detail="Station not found")
    return data

@router.get("/api/seismic/plot-data/{station_id}")
async def get_plot_data(station_id: str, trace_index: int = 0):
    """Данные для графика"""
    data = seed_handler.get_station_detail(station_id)
    if not data or not data.get('traces'):
        raise HTTPException(status_code=404, detail="No data found")
    
    trace = data['traces'][min(trace_index, len(data['traces'])-1)]
    
    raw_data = trace['data']
    mean_value = sum(raw_data) / len(raw_data) if raw_data else 0
    normalized_data = [v - mean_value for v in raw_data] 
    
    return {
        'channel': trace['channel'],
        'sampling_rate': trace['sampling_rate'],
        'starttime': trace['starttime'],
        'data': normalized_data, 
        'stats': {
            'min': min(normalized_data) if normalized_data else 0,
            'max': max(normalized_data) if normalized_data else 0,
            'mean': 0,
            'std': trace['std']
        }
    }

@router.websocket("/ws/seismic")
async def seismic_websocket(websocket: WebSocket):
    """WebSocket для real-time обновлений"""
    await ws_manager.connect(websocket)
    try:
        while True:
            new_data = seed_handler.process_new_files()
            if new_data:
                await ws_manager.broadcast({
                    'type': 'new_data',
                    'stations': list(new_data.keys()),
                    'timestamp': datetime.now().isoformat()
                })
            await asyncio.sleep(5)
    except Exception as e:
        print(f"WebSocket ошибка: {e}")
        ws_manager.disconnect(websocket)

async def auto_check_seed_files():
    while True:
        try:
            new_data = seed_handler.process_new_files()
            if new_data:
                await ws_manager.broadcast({
                    'type': 'files_updated',
                    'count': len(new_data),
                    'stations': list(new_data.keys())
                })
        except Exception as e:
            print(f"Ошибка автопроверки: {e}")
        await asyncio.sleep(10)

@router.on_event("startup")
async def startup():
    asyncio.create_task(auto_check_seed_files())
