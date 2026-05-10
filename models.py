from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

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
    role = Column(String(20), nullable=False, default="user")
    full_name = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    last_login = Column(DateTime)
    login_count = Column(Integer, default=0)