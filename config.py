import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

# ======================================================
# БАЗА ДАННЫХ
# ======================================================
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "noiselevel"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "client_encoding": "UTF8",
    "options": "-c client_encoding=UTF8"
}

DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    f"?client_encoding=UTF8"
)

# ======================================================
# JWT
# ======================================================
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "fallback-secret-key")
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 480

# ======================================================
# СЕРВЕР
# ======================================================
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

# ======================================================
# БАЙКАЛ-8 SEEDLINK
# ======================================================
SEEDLINK_ADDRESS = os.getenv("SEEDLINK_ADDRESS", "192.168.1.10:18000")
SEEDLINK_SELECT = os.getenv("SEEDLINK_SELECT", "NT_B8:CH3")

# ======================================================
# ДИРЕКТОРИИ
# ======================================================
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
MINISEED_DIR = DATA_DIR / "miniseed"
BAYKAL_ARCHIVE_DIR = DATA_DIR / "baykal_archive"

DATA_DIR.mkdir(exist_ok=True)
MINISEED_DIR.mkdir(exist_ok=True)
BAYKAL_ARCHIVE_DIR.mkdir(exist_ok=True)

SENSOR_FOLDERS = {
    "sensor_1": str(MINISEED_DIR / "sensor_1"),
    "sensor_2": str(MINISEED_DIR / "sensor_2"),
    "sensor_3": str(MINISEED_DIR / "sensor_3"),
    "sensor_4": str(MINISEED_DIR / "sensor_4"),
}

# ======================================================
# ПОЛЬЗОВАТЕЛИ ПО УМОЛЧАНИЮ
# ======================================================
DEFAULT_USERS = [
    {
        "login": os.getenv("ADMIN_LOGIN", "admin"),
        "password": os.getenv("ADMIN_PASSWORD", "admin123"),
        "role": "admin",
        "full_name": "Администратор системы"
    },
    {
        "login": os.getenv("VIEWER_LOGIN", "viewer"),
        "password": os.getenv("VIEWER_PASSWORD", "viewer123"),
        "role": "user",
        "full_name": "Оператор мониторинга"
    }
]

# ======================================================
# МАППИНГ СЕНСОРОВ
# ======================================================
SENSOR_ID_MAPPING = {
    "sensor_1": 1,
    "sensor_2": 5,
    "sensor_3": 6,
    "sensor_4": 2,
}

BAYKAL_SENSOR_DB_ID = int(os.getenv("BAYKAL_SENSOR_DB_ID", "7"))

# ======================================================
# ПОРОГИ АНОМАЛИЙ
# ======================================================
ANOMALY_THRESHOLDS = {
    "noise_level": {"warning": 55.0, "danger": 70.0, "min": 30.0, "max": 120.0},
    "vibration": {"danger": 70.0}
}