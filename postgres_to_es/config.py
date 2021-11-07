from typing import Optional, List
from pydantic import BaseSettings, SecretStr


class Settings(BaseSettings):
    PG_HOST: str
    PG_PORT: int
    PG_DBNAME: str
    PG_PASSWORD: SecretStr
    PG_USER: str
    LIMIT: Optional[int]
    ORDER_FIELD: List[str]
    STATE_FIELD: List[str]
    FETCH_DELAY: Optional[float]
    STATE_FILE_PATH: str
    INDICES_FILE_PATH: str
    LOGGER_LEVEL: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


config = Settings()