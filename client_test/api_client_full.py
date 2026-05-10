"""
Полный клиент API для геоэкологического мониторинга
Поддерживает ВСЕ эндпоинты сервера
"""
import requests
from typing import List, Dict, Optional, Any
from datetime import datetime

class GeoMonitoringClient:
    """
    Универсальный клиент для работы с сервером мониторинга.
    Заменяет прямое подключение к БД.
    
    Использование:
        client = GeoMonitoringClient("http://26.71.93.20:8000")
        client.login("viewer", "viewer123")
        sensors = client.sensors.get_all()
    """
    
    def __init__(self, server_url: str = "http://26.71.93.20:8000"):
        self.server_url = server_url.rstrip('/')
        self.token = None
        self.session = requests.Session()
        self._user = None
        
        # Подклассы для группировки методов
        self.sensors = self.SensorsAPI(self)
        self.measurements = self.MeasurementsAPI(self)
        self.anomalies = self.AnomaliesAPI(self)
        self.vibration = self.VibrationAPI(self)
        self.auth = self.AuthAPI(self)
        self.admin = self.AdminAPI(self) if False else None  # активируется после логина
    
    # ============================================================
    # ВНУТРЕННИЙ МЕТОД ДЛЯ ЗАПРОСОВ
    # ============================================================
    def _get(self, path: str, params: dict = None) -> dict:
        """GET запрос с обработкой ошибок"""
        try:
            response = self.session.get(
                f"{self.server_url}{path}",
                params=params,
                timeout=10
            )
            if response.status_code == 401:
                return {"error": "Требуется авторизация", "status_code": 401}
            return response.json()
        except requests.exceptions.ConnectionError:
            return {"error": "Сервер недоступен. Проверьте VPN подключение."}
        except Exception as e:
            return {"error": str(e)}
    
    def _post(self, path: str, data: dict = None) -> dict:
        """POST запрос с обработкой ошибок"""
        try:
            response = self.session.post(
                f"{self.server_url}{path}",
                json=data or {},
                timeout=10
            )
            return response.json()
        except requests.exceptions.ConnectionError:
            return {"error": "Сервер недоступен. Проверьте VPN подключение."}
        except Exception as e:
            return {"error": str(e)}
    
    # ============================================================
    # ПРОВЕРКА СВЯЗИ
    # ============================================================
    def health_check(self) -> dict:
        """Проверка доступности сервера и БД"""
        return self._get("/api/health")
    
    # ============================================================
    # ВЛОЖЕННЫЕ КЛАССЫ ПО КАТЕГОРИЯМ
    # ============================================================
    
    class SensorsAPI:
        """Работа с датчиками"""
        def __init__(self, client):
            self.client = client
        
        def get_all(self) -> List[Dict]:
            """Список всех датчиков"""
            return self.client._get("/api/sensors")
        
        def get_one(self, sensor_id: str) -> Dict:
            """Информация о конкретном датчике"""
            return self.client._get(f"/api/sensors/{sensor_id}")
        
        def get_count(self) -> Dict:
            """Количество датчиков (active/inactive)"""
            return self.client._get("/api/sensors/count")
        
        def get_stats(self, sensor_id: str) -> Dict:
            """Статистика датчика (средний шум, всего измерений)"""
            return self.client._get(f"/api/sensors/{sensor_id}/stats")
        
        def get_violations(self, sensor_id: str, hours: int = 24) -> Dict:
            """Нарушения по датчику за период"""
            return self.client._get(
                f"/api/sensors/{sensor_id}/violations",
                params={"hours": hours}
            )
    
    class MeasurementsAPI:
        """Работа с измерениями"""
        def __init__(self, client):
            self.client = client
        
        def get_latest(self) -> List[Dict]:
            """Последние измерения всех датчиков"""
            return self.client._get("/api/measurements/latest")
        
        def get_latest_all(self) -> Dict:
            """Все последние измерения (формат PostgreSQL)"""
            return self.client._get("/api/measurements/latest-all")
        
        def get_by_sensor(self, sensor_id: str, limit: int = 50, hours: int = None) -> List[Dict]:
            """Измерения конкретного датчика"""
            params = {"limit": limit}
            if hours:
                params["hours"] = hours
            return self.client._get(
                f"/api/sensors/{sensor_id}/measurements",
                params=params
            )
        
        def get_history(self, sensor_id: str, limit: int = 20) -> Dict:
            """История измерений датчика (второй формат)"""
            return self.client._get(
                f"/api/measurements/history/{sensor_id}",
                params={"limit": limit}
            )
        
        def get_stats(self, hours: int = 24) -> Dict:
            """Статистика измерений (средний шум, макс/мин, аномалии)"""
            return self.client._get(
                "/api/measurements/stats",
                params={"hours": hours}
            )
        
        def get_count(self, hours: int = 24) -> Dict:
            """Количество измерений за период"""
            return self.client._get(
                "/api/measurements/count",
                params={"hours": hours}
            )
        
        def get_raw_chunk(self, sensor_id: int = 7) -> Dict:
            """Последний чанк сырых данных"""
            return self.client._get(f"/api/sensors/{sensor_id}/raw-stream/latest")
    
    class AnomaliesAPI:
        """Работа с аномалиями"""
        def __init__(self, client):
            self.client = client
        
        def get_recent(self, limit: int = 10) -> Dict:
            """Последние аномалии (день/ночь отдельно)"""
            return self.client._get(
                "/api/anomalies/recent",
                params={"limit": limit}
            )
        
        def get_today(self) -> Dict:
            """Количество аномалий за сегодня"""
            return self.client._get("/api/anomalies/today")
        
        def get_stats(self) -> Dict:
            """Полная статистика аномалий"""
            return self.client._get("/api/anomalies/stats")
    
    class VibrationAPI:
        """Работа с вибрацией (Байкал-8)"""
        def __init__(self, client):
            self.client = client
        
        def get_latest(self) -> Dict:
            """Последнее значение вибрации"""
            return self.client._get("/api/vibration/latest")
        
        def get_baikal_latest(self) -> Dict:
            """Последнее значение из архива Байкал"""
            return self.client._get("/api/baikal/vibration/latest")
        
        def get_baikal_history(self, limit: int = 10) -> Dict:
            """История вибраций из архива"""
            return self.client._get(
                "/api/baikal/vibration/history",
                params={"limit": limit}
            )
        
        def get_baikal_stats(self, hours: int = 24) -> Dict:
            """Статистика вибраций"""
            return self.client._get(
                "/api/baikal/vibration/stats",
                params={"hours": hours}
            )
    
    class AuthAPI:
        """Авторизация"""
        def __init__(self, client):
            self.client = client
        
        def login(self, login: str, password: str) -> Dict:
            """Вход в систему, получение JWT токена"""
            result = self.client._post("/api/auth/login", {
                "login": login,
                "password": password
            })
            
            if "access_token" in result:
                self.client.token = result['access_token']
                self.client._user = result.get('user', {})
                self.client.session.headers.update({
                    'Authorization': f'Bearer {self.client.token}'
                })
                # Активируем админские методы
                if result.get('user', {}).get('role') == 'admin':
                    self.client.admin = self.client.AdminAPI(self.client)
            
            return result
        
        def get_me(self) -> Dict:
            """Информация о текущем пользователе"""
            return self.client._get("/api/auth/me")
        
        def logout(self):
            """Выход (сброс токена)"""
            self.client.token = None
            self.client._user = None
            self.client.session.headers.pop('Authorization', None)
            self.client.admin = None
    
    class AdminAPI:
        """Админские функции (только для admin)"""
        def __init__(self, client):
            self.client = client
        
        def get_users(self) -> List[Dict]:
            """Список всех пользователей"""
            return self.client._get("/api/admin/users")
        
        def create_user(self, login: str, password: str, role: str = "user", full_name: str = "") -> Dict:
            """Создать нового пользователя"""
            return self.client._post("/api/admin/users", {
                "login": login,
                "password": password,
                "role": role,
                "full_name": full_name
            })
        
        def deactivate_user(self, user_id: int) -> Dict:
            """Деактивировать пользователя"""
            response = self.client.session.delete(
                f"{self.client.server_url}/api/admin/users/{user_id}"
            )
            return response.json()
        
        def change_role(self, user_id: int, new_role: str) -> Dict:
            """Изменить роль пользователя"""
            response = self.client.session.put(
                f"{self.client.server_url}/api/admin/users/{user_id}/role",
                json={"role": new_role}
            )
            return response.json()
        
        def get_stats(self) -> Dict:
            """Расширенная статистика (датчики, измерения, пользователи)"""
            return self.client._get("/api/admin/stats")
    
    
    # ============================================================
    # MINISEED
    # ============================================================
    def get_mseed_stations(self) -> Dict:
        """Список станций miniSEED"""
        return self._get("/api/mseed/stations")
    
    def get_mseed_status(self) -> Dict:
        """Статус мониторинга miniSEED"""
        return self._get("/api/mseed/status")
    
    # ============================================================
    # ИНФО О ТЕКУЩЕМ ПОЛЬЗОВАТЕЛЕ
    # ============================================================
    @property
    def current_user(self) -> Optional[Dict]:
        """Данные авторизованного пользователя"""
        return self._user


# ================================================================
# ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ
# ================================================================
if __name__ == "__main__":
    client = GeoMonitoringClient("http://26.71.93.20:8000")
    
    # --- ПРОВЕРКА СВЯЗИ ---
    health = client.health_check()
    print(f"📡 Сервер: {health.get('status')}")
    
    # --- АВТОРИЗАЦИЯ ---
    login_result = client.auth.login("viewer", "viewer123")
    print(f"👤 Пользователь: {client.current_user}")
    
    # --- ДАТЧИКИ ---
    sensors = client.sensors.get_all()
    print(f"\n📊 Датчики: {len(sensors)} шт.")
    
    sensor_info = client.sensors.get_one("noise_sensor_001")
    print(f"📍 Датчик: {sensor_info.get('name')}")
    
    # --- ИЗМЕРЕНИЯ ---
    latest = client.measurements.get_latest()
    print(f"📈 Последних измерений: {len(latest)}")
    
    stats = client.measurements.get_stats(hours=24)
    print(f"📉 Статистика: средний шум {stats.get('average_noise')}")
    
    # --- АНОМАЛИИ ---
    anomalies = client.anomalies.get_today()
    print(f"⚠️ Аномалий сегодня: {anomalies.get('anomalies_today')}")
    
    anom_recent = client.anomalies.get_recent(limit=3)
    print(f"🔴 Дневных аномалий: {anom_recent.get('day', {}).get('count', 0)}")
    print(f"🌙 Ночных аномалий: {anom_recent.get('night', {}).get('count', 0)}")
    
    # --- ВИБРАЦИЯ ---
    vib = client.vibration.get_baikal_latest()
    print(f"🔧 Вибрация Байкал: {vib.get('vibration_level', 'N/A')}")
    
    
    # --- АДМИНКА (если залогинились как admin) ---
    # client.auth.login("admin", "admin123")
    # users = client.admin.get_users()
    # print(f"👥 Пользователей в системе: {len(users)}")
    
    # print(f"📨 Заявка: {result}")