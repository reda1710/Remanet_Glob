import os
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache

class Settings(BaseSettings):
    # MongoDB settings
    MONGODB_URI: str = Field(
        default="mongodb+srv://user:password@host/db",
        env="MONGODB_URI"
    )
    MONGODB_DB_NAME: str = Field(default="DataIngestion", env="MONGODB_DB_NAME")
    
    # Data thresholds
    MAX_T_GUN: float = Field(default=10.0, env="MAX_T_GUN")
    MAX_P_GUN: float = Field(default=10.0, env="MAX_P_GUN")
    MAX_Q_PG_N2: float = Field(default=10.0, env="MAX_Q_PG_N2")
    MAX_V_PARTICULE: float = Field(default=10.0, env="MAX_V_PARTICULE")
    
    # Maintenance thresholds
    T_GUN_MAINTENANCE_THRESHOLD: float = Field(default=10.0, env="T_GUN_MAINTENANCE_THRESHOLD")
    P_GUN_MAINTENANCE_THRESHOLD: float = Field(default=10.0, env="P_GUN_MAINTENANCE_THRESHOLD")
    Q_PG_N2_MAINTENANCE_THRESHOLD: float = Field(default=10.0, env="Q_PG_N2_MAINTENANCE_THRESHOLD")
    V_PARTICULE_MAINTENANCE_THRESHOLD: float = Field(default=10.0, env="V_PARTICULE_MAINTENANCE_THRESHOLD")
    
    # WebSocket settings
    BROADCAST_INTERVAL: int = Field(default=1, env="BROADCAST_INTERVAL")  # seconds
    
    class Config:
        env_file = ".env"
        case_sensitive = True

@lru_cache()
def get_settings():
    return Settings()
