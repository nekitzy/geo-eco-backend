import asyncio
import logging
import os
import secrets
import glob
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from pathlib import Path

# FastAPI
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from contextlib import asynccontextmanager

# Pydantic
from pydantic import BaseModel

# SQLAlchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey, func, JSON, text
from sqlalchemy.orm import sessionmaker, declarative_base, relationship

# PostgreSQL
import psycopg2
from psycopg2.extras import RealDictCursor

# NumPy
import numpy as np

# ObsPy для miniSEED
from obspy import read as obspy_read

# dotenv
from dotenv import load_dotenv

# JWT и хеширование
from passlib.context import CryptContext
from jose import JWTError, jwt

# Импорты из твоих модулей
from seismic_data_handler import router as seismic_router
from config import (
    DB_CONFIG, DATABASE_URL, JWT_SECRET_KEY, JWT_ALGORITHM,
    ACCESS_TOKEN_EXPIRE_MINUTES, SENSOR_FOLDERS, BAYKAL_ARCHIVE_DIR,
    SENSOR_ID_MAPPING, BAYKAL_SENSOR_DB_ID, DEFAULT_USERS,
    SEEDLINK_ADDRESS, SEEDLINK_SELECT
)
from mseed_monitor import MSEED_MONITOR
from baikal_vibration_monitor import BAIKAL_VIBRATION_MONITOR

load_dotenv()
# ======================================================
# ЛОГГИРОВАНИЕ
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ======================================================
# КОНФИГУРАЦИЯ БД
# ======================================================
POSTGRES_CONFIG = DB_CONFIG

try:
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT version();")
    db_version = cur.fetchone()
    cur.close()
    conn.close()
    engine = create_engine(DATABASE_URL)
    logger.info("✅ Подключено к PostgreSQL")
except Exception as e:
    logger.warning(f"⚠️ PostgreSQL недоступен, использую SQLite: {e}")
    DATABASE_URL = "sqlite:///./geo_monitoring.db"
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ======================================================
# МОДЕЛИ БД
# ======================================================
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
    installation_date = Column(DateTime, default=datetime.utcnow)
    last_maintenance = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

    measurements = relationship("Measurement", back_populates="sensor", cascade="all, delete-orphan")
    raw_chunks = relationship("SensorRawChunk", back_populates="sensor", cascade="all, delete-orphan")


class Measurement(Base):
    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"), nullable=False)
    noise_level = Column(Float)
    temperature = Column(Float)
    humidity = Column(Float)
    #	pressure = Column(Float)	#
    #	wind_speed = Column(Float)	#
    air_quality_index = Column(Integer)
    vibration_level = Column(Float)
    measured_at = Column(DateTime, default=datetime.utcnow, index=True)
    is_anomaly = Column(Boolean, default=False)
    anomaly_type = Column(String(50))

    sensor = relationship("Sensor", back_populates="measurements")


class SensorRawChunk(Base):
    __tablename__ = "sensor_raw_chunks"

    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"), nullable=False, index=True)
    chunk_start = Column(DateTime, nullable=False)
    chunk_end = Column(DateTime, nullable=False)
    sampling_rate = Column(Float, nullable=False)
    raw_values = Column(JSON, nullable=False)

    sensor = relationship("Sensor", back_populates="raw_chunks")


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    login = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(20), nullable=False, default="user")  # "user" или "admin"
    full_name = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    last_login = Column(DateTime)
    login_count = Column(Integer, default=0)


Base.metadata.create_all(bind=engine)

# ======================================================
# БЕЗОПАСНОСТЬ (JWT + хеширование паролей)
# ======================================================
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

SECRET_KEY = JWT_SECRET_KEY
ALGORITHM = JWT_ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = ACCESS_TOKEN_EXPIRE_MINUTES # 8h

security = HTTPBearer()


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(credentials: HTTPAuthorizationCredentials = Security(security)) -> User:
    """Получить текущего пользователя из JWT токена"""
    credentials_exception = HTTPException(
        status_code=401,
        detail="Недействительный токен авторизации",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("user_id")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id, User.is_active == True).first()
        if user is None:
            raise credentials_exception
        return user
    finally:
        db.close()


async def get_admin_user(current_user: User = Depends(get_current_user)) -> User:
    """Проверить, что пользователь - админ"""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Требуются права администратора")
    return current_user


def init_default_users():
    """Создать дефолтных пользователей, если их нет"""
    db = SessionLocal()
    try:
        user_count = db.query(User).count()
        if user_count > 0:
            logger.info(f"👥 Пользователи уже существуют: {user_count}")
            return

        for user_data in DEFAULT_USERS:
            user = User(
                login=user_data["login"],
                password_hash=hash_password(user_data["password"]),
                role=user_data["role"],
                full_name=user_data["full_name"],
                is_active=True
            )
            db.add(user)

        db.commit()
        logger.info("✅ Созданы пользователи по умолчанию:")
        logger.info(f"   🔑 {DEFAULT_USERS[0]['login']} / {DEFAULT_USERS[0]['password']} (роль: admin)")
        logger.info(f"   👤 {DEFAULT_USERS[1]['login']} / {DEFAULT_USERS[1]['password']} (роль: user)")

    except Exception as e:
        logger.error(f"❌ Ошибка создания пользователей: {e}")
        db.rollback()
    finally:
        db.close()

# Инициализируем пользователей
init_default_users()

# ======================================================
# PYDANTIC СХЕМЫ
# ======================================================
class SensorResponse(BaseModel):
    id: int
    sensor_id: str
    name: str
    description: Optional[str] = None
    latitude: float
    longitude: float
    address: Optional[str] = None
    sensor_type: str
    status: str
    installation_date: Optional[datetime] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class MeasurementResponse(BaseModel):
    id: int
    sensor_id: int
    noise_level: Optional[float] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    #	pressure: Optional[float] = None	#
    #	wind_speed: Optional[float] = None	#
    air_quality_index: Optional[int] = None
    vibration_level: Optional[float] = None
    measured_at: datetime
    is_anomaly: bool
    anomaly_type: Optional[str] = None

    class Config:
        from_attributes = True


class UserLogin(BaseModel):
    login: str
    password: str


class UserResponse(BaseModel):
    id: int
    login: str
    role: str
    full_name: Optional[str] = None
    is_active: bool
    created_at: Optional[datetime] = None
    last_login: Optional[datetime] = None

    class Config:
        from_attributes = True


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse



# ======================================================
# WEB-SOCKET МЕНЕДЖЕР
# ======================================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)


manager = ConnectionManager()

# ======================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ======================================================
def calculate_noise_db(data: np.ndarray) -> float:
    """Вычисление уровня шума в дБ"""
    if len(data) == 0:
        return 0.0
    rms = np.sqrt(np.mean(data ** 2))
    db = 20 * np.log10(rms + 1e-10)
    return float(np.clip(db + 55, 30, 90))


#BAYKAL_ARCHIVE_DIR = r"C:\baykal-control\archive"
#BAYKAL_SENSOR_DB_ID = 7


def determine_zone_type_from_features(features: dict) -> str:
    if features['road_distance'] < 0.3:
        return "Дорога/Трасса"
    elif features['commercial_distance'] < 0.3:
        return "Торговый центр"
    elif features['school_distance'] < 0.3:
        return "Учебное заведение"
    elif features['park_distance'] < 0.3:
        return "Парк/Зелёная зона"
    elif features['residential_distance'] < 0.3:
        return "Жилая зона"
    return "Городская зона"


def get_peak_hours(zone_type: str) -> str:
    peak_map = {
        "Дорога/Трасса": "07:00-09:00, 17:00-19:00",
        "Торговый центр": "12:00-14:00, 18:00-20:00",
        "Учебное заведение": "08:00-10:00, 14:00-16:00",
        "Парк/Зелёная зона": "10:00-12:00, 16:00-18:00",
        "Жилая зона": "07:00-09:00, 19:00-22:00"
    }
    return peak_map.get(zone_type, "08:00-10:00, 17:00-19:00")


# Новостной кэш
news_cache = {
    "data": [],
    "timestamp": None,
    "expires_in": 1800
}


async def fetch_news_from_sources():
    """Загрузка новостей"""
    all_news = []
    rss_sources = [
        {"url": "https://www.nsu.ru/n/portal/news/rss.xml", "source": "НГУ", "category": "Наука"},
        {"url": "https://www.sbras.info/rss", "source": "СО РАН", "category": "Наука"},
        {"url": "https://tayga.info/rss", "source": "Тайга.инфо", "category": "Экология"},
        {"url": "https://www.1news.ru/news/rss/", "source": "1News", "category": "Новосибирск"},
    ]
    eco_keywords = [
        "экология", "мониторинг", "шум", "воздух", "загрязнение",
        "Академгородок", "НГУ", "СО РАН", "окружающая среда",
        "климат", "природа", "защита", "измерение", "датчик"
    ]

    async with aiohttp.ClientSession() as session:
        for source in rss_sources:
            try:
                async with session.get(source["url"], timeout=10) as response:
                    if response.status == 200:
                        rss_content = await response.text()
                        feed = feedparser.parse(rss_content)
                        for entry in feed.entries[:5]:
                            title = entry.get('title', '').lower()
                            summary = entry.get('summary', '').lower()
                            is_relevant = any(keyword in title or keyword in summary for keyword in eco_keywords)
                            if is_relevant or source["category"] == "Экология":
                                published = entry.get('published_parsed')
                                pub_date = datetime(*published[:6]) if published else datetime.now()
                                all_news.append({
                                    "id": f"{source['source']}_{len(all_news)}",
                                    "title": entry.get('title', 'Без названия'),
                                    "summary": entry.get('summary', '')[:200] + '...',
                                    "source": source["source"],
                                    "category": source["category"],
                                    "url": entry.get('link', '#'),
                                    "published_at": pub_date.isoformat(),
                                    "image": "🌳" if source["category"] == "Экология" else "🔬",
                                    "is_geoecology": is_relevant
                                })
            except Exception as e:
                logger.error(f"Ошибка новостей {source['source']}: {e}")
                continue

    all_news.sort(key=lambda x: x["published_at"], reverse=True)
    return all_news[:10]


def get_curated_geoecology_news():
    return [
        {
            "id": "curated_1",
            "title": "Система мониторинга шума запущена в Академгородке",
            "summary": "Новая система геоэкологического мониторинга начала работу в Новосибирском Академгородке. Датчики установлены в ключевых точках района.",
            "source": "ГеоМониАкадем",
            "category": "Экология",
            "url": "#",
            "published_at": (datetime.now() - timedelta(days=2)).isoformat(),
            "image": "📡",
            "is_geoecology": True
        },
        {
            "id": "curated_2",
            "title": "ИВМиМГ СО РАН внедряет технологии экологического контроля",
            "summary": "Институт вычислительной математики и математической геофизики Сибирского отделения РАН внедрил новые методы мониторинга окружающей среды с использованием датчиков.",
            "source": "СО РАН",
            "category": "Наука",
            "url": "https://www.sbras.info",
            "published_at": (datetime.now() - timedelta(days=5)).isoformat(),
            "image": "🔬",
            "is_geoecology": True
        },
    ]


# ======================================================
# LIFESPAN
# ======================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    for folder in SENSOR_FOLDERS.values():
        os.makedirs(folder, exist_ok=True)

    monitor_task = asyncio.create_task(MSEED_MONITOR.start_monitoring(interval=2.0))
    baikal_monitor_task = asyncio.create_task(BAIKAL_VIBRATION_MONITOR.start_monitoring(interval=5.0))

    seedlink_collector = None
    try:
        from baikal_seedlink_collector import BaikalSeedLinkCollector
        seedlink_collector = BaikalSeedLinkCollector(
            db_session_factory=SessionLocal,
            chunk_model_class=SensorRawChunk,
            sensor_id=BAYKAL_SENSOR_DB_ID,
            address=SEEDLINK_ADDRESS,
            select=SEEDLINK_SELECT,
            flush_interval=5.0
        )
        seedlink_collector.start()
    except Exception as e:
        logger.warning(f"⚠️ Не удалось запустить SeedLink сборщик: {e}")

    yield  # Сервер работает

    monitor_task.cancel()
    baikal_monitor_task.cancel()
    if seedlink_collector is not None:
        try:
            seedlink_collector.stop()
        except Exception as e:
            logger.error(f"Ошибка при остановке collector: {e}")
    MSEED_MONITOR.close()
    if BAIKAL_VIBRATION_MONITOR.db_connection:
        BAIKAL_VIBRATION_MONITOR.db_connection.close()


# ======================================================
# FASTAPI ПРИЛОЖЕНИЕ
# ======================================================
app = FastAPI(title="Система мониторинга шума Академгородка", lifespan=lifespan)

STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(seismic_router)

# ======================================================
# КОРНЕВЫЕ ЭНДПОИНТЫ
# ======================================================
@app.get("/")
async def read_root():
    return FileResponse("static/index.html")


@app.get("/api/health")
async def health_check():
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


# ======================================================
# WEB-SOCKET
# ======================================================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong", "timestamp": datetime.now().isoformat()})
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.websocket("/ws/mseed")
async def mseed_websocket(websocket: WebSocket):
    await MSEED_MONITOR.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except Exception:
        MSEED_MONITOR.disconnect(websocket)


@app.websocket("/ws/vibration")
async def vibration_websocket(websocket: WebSocket):
    await BAIKAL_VIBRATION_MONITOR.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception:
        BAIKAL_VIBRATION_MONITOR.disconnect(websocket)


@app.websocket("/ws/baikal/vibration")
async def baikal_vibration_websocket(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            latest = await get_baikal_vibration_latest()
            if latest.get("success"):
                await websocket.send_json({"type": "vibration_realtime", "data": latest})
            await asyncio.sleep(3)
    except Exception:
        pass


# ======================================================
# API — АВТОРИЗАЦИЯ И ПОЛЬЗОВАТЕЛИ
# ======================================================

@app.post("/api/auth/login", response_model=TokenResponse)
async def login(user_data: UserLogin):
    """Вход в систему"""
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.login == user_data.login).first()

        if not user or not verify_password(user_data.password, user.password_hash):
            raise HTTPException(status_code=401, detail="Неверный логин или пароль")

        if not user.is_active:
            raise HTTPException(status_code=403, detail="Пользователь заблокирован")

        # Обновляем статистику
        user.last_login = datetime.utcnow()
        user.login_count = (user.login_count or 0) + 1
        db.commit()

        # Создаём токен
        access_token = create_access_token(data={"user_id": user.id, "role": user.role})

        return {
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": user.id,
                "login": user.login,
                "role": user.role,
                "full_name": user.full_name,
                "is_active": user.is_active,
                "created_at": user.created_at,
                "last_login": user.last_login
            }
        }
    finally:
        db.close()


@app.get("/api/auth/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    """Получить информацию о текущем пользователе"""
    return current_user


@app.get("/api/admin/users", response_model=List[UserResponse])
async def get_all_users(admin: User = Depends(get_admin_user)):
    """Получить список всех пользователей (только админ)"""
    db = SessionLocal()
    try:
        users = db.query(User).all()
        return users
    finally:
        db.close()


@app.post("/api/admin/users")
async def create_user(
    user_data: dict,
    admin: User = Depends(get_admin_user)
):
    """Создать нового пользователя (только админ)"""
    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.login == user_data["login"]).first()
        if existing:
            raise HTTPException(status_code=400, detail="Пользователь с таким логином уже существует")

        user = User(
            login=user_data["login"],
            password_hash=hash_password(user_data["password"]),
            role=user_data.get("role", "user"),
            full_name=user_data.get("full_name", ""),
            is_active=True
        )
        db.add(user)
        db.commit()
        db.refresh(user)

        logger.info(f"✅ Создан пользователь: {user.login} (роль: {user.role})")
        return {"success": True, "user_id": user.id, "login": user.login}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/admin/users/{user_id}")
async def deactivate_user(
    user_id: int,
    admin: User = Depends(get_admin_user)
):
    """Деактивировать пользователя (только админ)"""
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="Пользователь не найден")

        if user.id == admin.id:
            raise HTTPException(status_code=400, detail="Нельзя деактивировать самого себя")

        user.is_active = False
        db.commit()

        return {"success": True, "message": f"Пользователь {user.login} деактивирован"}
    finally:
        db.close()


@app.put("/api/admin/users/{user_id}/role")
async def change_user_role(
    user_id: int,
    role_data: dict,
    admin: User = Depends(get_admin_user)
):
    """Изменить роль пользователя (только админ)"""
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="Пользователь не найден")

        if user.id == admin.id:
            raise HTTPException(status_code=400, detail="Нельзя изменить свою роль")

        new_role = role_data.get("role")
        if new_role not in ["user", "admin"]:
            raise HTTPException(status_code=400, detail="Роль должна быть 'user' или 'admin'")

        user.role = new_role
        db.commit()

        return {"success": True, "message": f"Роль пользователя {user.login} изменена на {new_role}"}
    finally:
        db.close()


@app.get("/api/admin/stats")
async def get_admin_stats(admin: User = Depends(get_admin_user)):
    """Расширенная статистика (только админ)"""
    db = SessionLocal()
    try:
        total_sensors = db.query(Sensor).count()
        total_measurements = db.query(Measurement).count()
        users_count = db.query(User).count()

        return {
            "sensors": total_sensors,
            "measurements": total_measurements,
            "users": users_count,
            "timestamp": datetime.now().isoformat()
        }
    finally:
        db.close()


# ======================================================
# API — ДАТЧИКИ
# ======================================================
@app.get("/api/sensors", response_model=List[SensorResponse])
async def get_all_sensors():
    """Получить список всех датчиков"""
    db = SessionLocal()
    try:
        sensors = db.query(Sensor).all()
        return sensors
    finally:
        db.close()


@app.get("/api/sensors/{sensor_id}", response_model=SensorResponse)
async def get_sensor_by_id(sensor_id: str):
    """Получить информацию о конкретном датчике"""
    db = SessionLocal()
    try:
        sensor = db.query(Sensor).filter(Sensor.sensor_id == sensor_id).first()
        if not sensor:
            raise HTTPException(status_code=404, detail="Датчик не найден")
        return sensor
    finally:
        db.close()


@app.get("/api/sensors/count")
async def get_sensors_count():
    """Получить количество датчиков"""
    db = SessionLocal()
    try:
        total = db.query(Sensor).count()
        active = db.query(Sensor).filter(Sensor.status == "active").count()
        return {"total": total, "active": active, "inactive": total - active}
    finally:
        db.close()


@app.get("/api/sensors/{sensor_id}/stats")
async def get_sensor_statistics(sensor_id: str):
    """Получить статистику по датчику"""
    try:
        SENSOR_ID_MAP = {
            "sensor_1": "noise_sensor_001",
            "sensor_2": "noise_sensor_002",
            "sensor_3": "noise_sensor_003",
            "sensor_4": "noise_sensor_004",
        }
        db_sensor_id_str = SENSOR_ID_MAP.get(sensor_id, sensor_id)
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, sensor_id, name, address, sensor_type FROM sensors WHERE sensor_id = %s", (db_sensor_id_str,))
        sensor_info = cur.fetchone()
        if not sensor_info:
            cur.close()
            conn.close()
            return {"success": False, "error": "Датчик не найден"}
        db_sensor_id = sensor_info['id']
        cur.execute("""
            SELECT AVG(noise_level) as average_noise, COUNT(*) as total_measurements, MAX(measured_at) as last_measurement
            FROM measurements WHERE sensor_id = %s AND noise_level IS NOT NULL
        """, (db_sensor_id,))
        stats = cur.fetchone()
        cur.close()
        conn.close()
        return {
            "success": True,
            "sensor_id": sensor_id,
            "sensor_name": sensor_info['name'],
            "address": sensor_info['address'] or 'Адрес не указан',
            "sensor_type": sensor_info['sensor_type'] or 'noise',
            "average_noise": float(stats['average_noise']) if stats['average_noise'] else 0,
            "total_measurements": stats['total_measurements'] or 0,
            "last_measurement": stats['last_measurement'].isoformat() if stats['last_measurement'] else None
        }
    except Exception as e:
        return {"success": False, "error": str(e), "average_noise": 0}


# ======================================================
# API — ИЗМЕРЕНИЯ
# ======================================================
@app.get("/api/sensors/{sensor_id}/measurements")
async def get_sensor_measurements(sensor_id: str, limit: int = 50, hours: Optional[int] = None):
    """Получить измерения конкретного датчика"""
    db = SessionLocal()
    try:
        sensor = db.query(Sensor).filter(Sensor.sensor_id == sensor_id).first()
        if not sensor:
            raise HTTPException(status_code=404, detail="Датчик не найден")
        query = db.query(Measurement).filter(Measurement.sensor_id == sensor.id)
        if hours:
            time_threshold = datetime.utcnow() - timedelta(hours=hours)
            query = query.filter(Measurement.measured_at >= time_threshold)
        measurements = query.order_by(Measurement.measured_at.desc()).limit(limit).all()
        return [
            {
                "id": m.id,
                "noise_level": m.noise_level,
                "temperature": m.temperature,
                "humidity": m.humidity,
                #	"pressure": m.pressure,	#
                #	"wind_speed": m.wind_speed, 	#
                "air_quality_index": m.air_quality_index,
                "vibration_level": m.vibration_level,
                "measured_at": m.measured_at.isoformat(),
                "is_anomaly": m.is_anomaly,
                "anomaly_type": m.anomaly_type
            }
            for m in measurements
        ]
    finally:
        db.close()


@app.get("/api/measurements/latest")
async def get_latest_measurements_all():
    """Получить последние измерения всех датчиков"""
    db = SessionLocal()
    try:
        sensors = db.query(Sensor).all()
        results = []
        for sensor in sensors:
            latest = db.query(Measurement)\
                .filter(Measurement.sensor_id == sensor.id)\
                .order_by(Measurement.measured_at.desc())\
                .first()
            if latest:
                results.append({
                    "sensor_id": sensor.sensor_id,
                    "sensor_name": sensor.name,
                    "latitude": sensor.latitude,
                    "longitude": sensor.longitude,
                    "address": sensor.address,
                    "sensor_type": sensor.sensor_type,
                    "status": sensor.status,
                    "noise_level": latest.noise_level,
                    "temperature": latest.temperature,
                    "humidity": latest.humidity,
                    #	"pressure": latest.pressure,		#
                    #	"wind_speed": latest.wind_speed,	#
                    "air_quality_index": latest.air_quality_index,
                    "vibration_level": latest.vibration_level,
                    "is_anomaly": latest.is_anomaly,
                    "anomaly_type": latest.anomaly_type,
                    "measured_at": latest.measured_at.isoformat()
                })
        return results
    finally:
        db.close()


@app.get("/api/measurements/stats")
async def get_measurement_statistics(hours: int = 24):
    """Получить статистику измерений"""
    db = SessionLocal()
    try:
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        noise_stats = db.query(
            func.avg(Measurement.noise_level).label("avg_noise"),
            func.max(Measurement.noise_level).label("max_noise"),
            func.min(Measurement.noise_level).label("min_noise"),
            func.count(Measurement.id).label("total_measurements")
        ).filter(
            Measurement.measured_at >= time_threshold,
            Measurement.noise_level.isnot(None)
        ).first()
        anomaly_count = db.query(Measurement)\
            .filter(Measurement.measured_at >= time_threshold)\
            .filter(Measurement.is_anomaly == True)\
            .count()
        sensor_count = db.query(func.count(func.distinct(Measurement.sensor_id)))\
            .filter(Measurement.measured_at >= time_threshold)\
            .scalar()
        return {
            "period_hours": hours,
            "average_noise": float(noise_stats.avg_noise or 0),
            "max_noise": float(noise_stats.max_noise or 0),
            "min_noise": float(noise_stats.min_noise or 0),
            "total_measurements": noise_stats.total_measurements or 0,
            "anomaly_count": anomaly_count,
            "active_sensors": sensor_count or 0,
            "timestamp": datetime.utcnow().isoformat()
        }
    finally:
        db.close()


@app.get("/api/measurements/count")
async def get_measurements_count(hours: int = 24):
    """Получить количество измерений за период"""
    db = SessionLocal()
    try:
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        count = db.query(Measurement).filter(Measurement.measured_at >= time_threshold).count()
        return {"count": count, "period_hours": hours}
    finally:
        db.close()


@app.get("/api/measurements/latest-all")
async def get_latest_all():
    """Получить последние измерения для всех датчиков (PostgreSQL)"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT DISTINCT ON (m.sensor_id)
                m.id, m.sensor_id, m.noise_level, m.temperature, m.humidity,
                m.air_quality_index, m.vibration_level, m.measured_at, m.is_anomaly, m.anomaly_type,
                s.name as sensor_name, s.latitude, s.longitude, s.sensor_type, s.status
            FROM measurements m
            JOIN sensors s ON s.id = m.sensor_id
            ORDER BY m.sensor_id, m.measured_at DESC
        """)
        results = cur.fetchall()
        cur.close()
        conn.close()
        return {
            "success": True,
            "data": [
                {
                    "sensor_id": f"sensor_{r['sensor_id']}",
                    "sensor_name": r['sensor_name'],
                    "latitude": float(r['latitude']),
                    "longitude": float(r['longitude']),
                    "sensor_type": r['sensor_type'],
                    "status": r['status'],
                    "noise_level": float(r['noise_level']) if r['noise_level'] else None,
                    "temperature": float(r['temperature']) if r['temperature'] else None,
                    "humidity": float(r['humidity']) if r['humidity'] else None,
                    "air_quality_index": r['air_quality_index'],
                    "vibration_level": float(r['vibration_level']) if r['vibration_level'] else None,
                    "measured_at": r['measured_at'].isoformat() if r['measured_at'] else None,
                    "is_anomaly": r['is_anomaly']
                }
                for r in results
            ]
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/measurements/history/{sensor_id}")
async def get_measurement_history(sensor_id: str, limit: int = 20):
    """Получить историю измерений датчика"""
    try:
        db_sensor_id = sensor_id.replace("sensor_", "")
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT noise_level, temperature, humidity, measured_at, is_anomaly
            FROM measurements WHERE sensor_id = %s
            ORDER BY measured_at DESC LIMIT %s
        """, (db_sensor_id, limit))
        results = cur.fetchall()
        cur.close()
        conn.close()
        return {
            "success": True,
            "sensor_id": sensor_id,
            "data": [
                {
                    "noise_level": float(r['noise_level']) if r['noise_level'] else None,
                    "temperature": float(r['temperature']) if r['temperature'] else None,
                    "humidity": float(r['humidity']) if r['humidity'] else None,
                    "measured_at": r['measured_at'].isoformat(),
                    "is_anomaly": r['is_anomaly']
                }
                for r in results
            ]
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ======================================================
# API — АНОМАЛИИ
# ======================================================
@app.get("/api/anomalies/recent")
async def get_recent_anomalies(limit: int = 10):
    """Получить последние аномалии (день/ночь)"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT m.id, m.sensor_id as db_sensor_id, s.sensor_id as external_sensor_id, s.name as sensor_name,
                   m.noise_level, m.anomaly_type, m.is_anomaly, m.measured_at
            FROM measurements m
            JOIN sensors s ON s.id = m.sensor_id
            WHERE m.is_anomaly = TRUE AND m.anomaly_type = 'danger_high_noise'
            ORDER BY m.measured_at DESC LIMIT %s
        """, (limit * 2,))
        results = cur.fetchall()
        cur.close()
        conn.close()

        day_anomalies = []
        night_anomalies = []
        for row in results:
            hour = row['measured_at'].hour if row['measured_at'] else 0
            entry = {
                "id": row['id'],
                "sensor_id": row['external_sensor_id'],
                "sensor_name": row['sensor_name'],
                "noise_level": float(row['noise_level']) if row['noise_level'] else None,
                "anomaly_type": row['anomaly_type'] or 'danger_high_noise',
                "measured_at": row['measured_at'].isoformat() if row['measured_at'] else None,
                "period": "night" if (hour >= 23 or hour < 6) else "day"
            }
            if entry["period"] == "day":
                day_anomalies.append(entry)
            else:
                night_anomalies.append(entry)

        return {
            "success": True,
            "day": {"count": len(day_anomalies[:limit]), "data": day_anomalies[:limit]},
            "night": {"count": len(night_anomalies[:limit]), "data": night_anomalies[:limit]},
            "total_danger": len(day_anomalies) + len(night_anomalies)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "day": {"count": 0, "data": []}, "night": {"count": 0, "data": []}}


@app.get("/api/anomalies/today")
async def get_anomalies_today():
    """Получить количество аномалий за сегодня"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT COUNT(*) as today_count FROM measurements
            WHERE is_anomaly = TRUE AND DATE(measured_at) = DATE(NOW())
        """)
        result = cur.fetchone()
        cur.close()
        conn.close()
        return {
            "success": True,
            "anomalies_today": result['today_count'] or 0,
            "date": datetime.now().strftime('%Y-%m-%d'),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"success": False, "error": str(e), "anomalies_today": 0}


@app.get("/api/anomalies/stats")
async def get_anomaly_statistics():
    """Получить статистику аномалий"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT COUNT(*) as total_anomalies FROM measurements WHERE is_anomaly = TRUE")
        total = cur.fetchone()['total_anomalies']
        cur.execute("SELECT COUNT(*) as danger_count FROM measurements WHERE is_anomaly = TRUE AND anomaly_type = 'danger_high_noise'")
        danger_count = cur.fetchone()['danger_count']
        cur.execute("""
            SELECT s.name as sensor_name, s.sensor_id, COUNT(*) as count
            FROM measurements m JOIN sensors s ON s.id = m.sensor_id
            WHERE m.is_anomaly = TRUE
            GROUP BY s.name, s.sensor_id ORDER BY count DESC LIMIT 10
        """)
        by_sensor = [dict(row) for row in cur.fetchall()]
        cur.close()
        conn.close()
        return {
            "total_anomalies": total,
            "danger_count": danger_count or 0,
            "by_sensor": by_sensor,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/sensors/{sensor_id}/violations")
async def get_sensor_violations(sensor_id: str, hours: int = 24):
    """Получить статистику нарушений по датчику"""
    try:
        SENSOR_ID_MAP = {
            "sensor_1": "noise_sensor_001",
            "sensor_2": "noise_sensor_002",
            "sensor_3": "noise_sensor_003",
            "sensor_4": "noise_sensor_004",
        }
        db_sensor_id_str = SENSOR_ID_MAP.get(sensor_id, sensor_id)
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id FROM sensors WHERE sensor_id = %s", (db_sensor_id_str,))
        sensor_record = cur.fetchone()
        if not sensor_record:
            cur.close(); conn.close()
            return {"success": False, "error": f"Датчик {sensor_id} не найден", "total": 0}
        db_sensor_id = sensor_record['id']
        cur.execute("SELECT COUNT(*) as total FROM measurements WHERE sensor_id = %s AND is_anomaly = TRUE AND measured_at >= NOW() - INTERVAL '%s hours'", (db_sensor_id, hours))
        total = cur.fetchone()['total']
        cur.execute("SELECT COUNT(*) as day_count FROM measurements WHERE sensor_id = %s AND is_anomaly = TRUE AND measured_at >= NOW() - INTERVAL '%s hours' AND EXTRACT(HOUR FROM measured_at) >= 6 AND EXTRACT(HOUR FROM measured_at) < 23", (db_sensor_id, hours))
        day_count = cur.fetchone()['day_count']
        cur.execute("SELECT COUNT(*) as night_count FROM measurements WHERE sensor_id = %s AND is_anomaly = TRUE AND measured_at >= NOW() - INTERVAL '%s hours' AND (EXTRACT(HOUR FROM measured_at) >= 23 OR EXTRACT(HOUR FROM measured_at) < 6)", (db_sensor_id, hours))
        night_count = cur.fetchone()['night_count']
        cur.close(); conn.close()
        return {
            "success": True,
            "sensor_id": sensor_id,
            "period_hours": hours,
            "total": total or 0,
            "day": day_count or 0,
            "night": night_count or 0
        }
    except Exception as e:
        return {"success": False, "error": str(e), "total": 0, "day": 0, "night": 0}


# ======================================================

# ======================================================
# API — MINISEED
# ======================================================
@app.get("/api/mseed/stations")
async def get_mseed_stations():
    """Список станций miniSEED"""
    return {
        "stations": [
            {
                "station_id": sid,
                "folder": folder,
                "processed_count": len(MSEED_MONITOR.processed_files.get(sid, set()))
            }
            for sid, folder in SENSOR_FOLDERS.items()
        ]
    }


@app.get("/api/mseed/status")
async def get_mseed_status():
    """Статус мониторинга miniSEED"""
    status = {"sensors": {}, "total_processed": 0, "websocket_clients": len(MSEED_MONITOR.websockets)}
    for sensor_id, folder in MSEED_MONITOR.folders.items():
        processed_count = len(MSEED_MONITOR.processed_files.get(sensor_id, set()))
        status["sensors"][sensor_id] = {
            "folder": folder,
            "processed_files": processed_count,
            "exists": os.path.exists(folder)
        }
        status["total_processed"] += processed_count
    return status


# ======================================================
# API — ВИБРАЦИЯ (БАЙКАЛ)
# ======================================================
@app.get("/api/vibration/stats")
async def get_vibration_stats():
    """Статистика вибраций"""
    return BAIKAL_VIBRATION_MONITOR.get_stats()


@app.get("/api/vibration/latest")
async def get_vibration_latest():
    """Последнее значение вибрации"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT vibration_level, measured_at, is_anomaly
            FROM measurements WHERE sensor_id = %s AND vibration_level IS NOT NULL
            ORDER BY measured_at DESC LIMIT 1
        """, (BAYKAL_SENSOR_DB_ID,))
        res = cur.fetchone()
        cur.close(); conn.close()
        return {
            "vibration_level": res['vibration_level'] if res else 0,
            "measured_at": res['measured_at'].isoformat() if res and res['measured_at'] else None,
            "is_anomaly": res['is_anomaly'] if res else False
        }
    except Exception:
        return {"vibration_level": 0, "measured_at": None, "is_anomaly": False}


@app.get("/api/baikal/vibration/latest")
async def get_baikal_vibration_latest():
    """Последнее значение вибрации Байкал-8 из архива"""
    try:
        pattern = os.path.join(BAYKAL_ARCHIVE_DIR, "*.seed")
        files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
        if not files:
            return {"success": False, "error": "No files found"}
        st = obspy_read(files[0])
        if not st:
            return {"success": False, "error": "Empty file"}
        trace = st[0]
        data = trace.data.astype(float)
        vibration = float(np.sqrt(np.mean(data ** 2)) / 1000.0)
        ts = trace.stats.starttime.datetime
        if ts.year < 2000:
            ts = datetime.fromtimestamp(os.path.getmtime(files[0]))
        return {
            "success": True,
            "vibration_level": round(vibration, 4),
            "timestamp": ts.isoformat(),
            "filename": os.path.basename(files[0]),
            "is_anomaly": vibration > 1.0
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/baikal/vibration/history")
async def get_baikal_vibration_history(limit: int = 10):
    """История вибраций Байкал-8"""
    try:
        pattern = os.path.join(BAYKAL_ARCHIVE_DIR, "*.seed")
        files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)[:limit]
        results = []
        for filepath in files:
            try:
                st = obspy_read(filepath)
                if not st: continue
                trace = st[0]
                data = trace.data.astype(float)
                if len(data) == 0: continue
                vibration = float(np.sqrt(np.mean(data ** 2)) / 1000.0)
                ts = trace.stats.starttime.datetime
                if ts.year < 2000:
                    ts = datetime.fromtimestamp(os.path.getmtime(filepath))
                results.append({
                    "timestamp": ts.isoformat(),
                    "vibration_level": round(vibration, 4),
                    "filename": os.path.basename(filepath)
                })
            except Exception:
                continue
        results.sort(key=lambda x: x["timestamp"])
        return {"success": True, "data": results[-limit:]}
    except Exception as e:
        return {"success": False, "error": str(e), "data": []}


@app.get("/api/baikal/vibration/stats")
async def get_baikal_vibration_stats(hours: int = 24):
    """Статистика вибраций Байкал"""
    try:
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT COUNT(*) as total FROM measurements WHERE sensor_id = %s AND is_anomaly = TRUE AND measured_at >= %s", (BAYKAL_SENSOR_DB_ID, time_threshold))
        total = cur.fetchone()['total'] or 0
        cur.close(); conn.close()
        return {"success": True, "total": total}
    except Exception as e:
        return {"success": False, "error": str(e), "total": 0}


# ======================================================
# API — RAW CHUNKS
# ======================================================
@app.get("/api/sensors/{sensor_id}/raw-stream/latest")
async def get_latest_raw_chunk(sensor_id: int):
    """Получить последний чанк сырых данных"""
    db = SessionLocal()
    try:
        chunk = db.query(SensorRawChunk)\
            .filter(SensorRawChunk.sensor_id == sensor_id)\
            .order_by(SensorRawChunk.chunk_start.desc())\
            .first()
        if not chunk:
            return {"success": False, "message": "Нет данных"}
        return {
            "success": True,
            "sensor_id": chunk.sensor_id,
            "start": chunk.chunk_start.isoformat(),
            "end": chunk.chunk_end.isoformat(),
            "sampling_rate": chunk.sampling_rate,
            "data": chunk.raw_values
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        db.close()



# ======================================================
# ЗАПУСК
# ======================================================
if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Запуск сервера геоэкологического мониторинга...")
    logger.info("👥 Пользователи для входа:")
    logger.info("   🔑 admin / admin123 (админ)")
    logger.info("   👤 viewer / viewer123 (только просмотр)")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")