from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_KEY: str
    APP_SECRET: str
    BASE_URL: str
    DB_HOST: str
    DB_PORT: int
    DB_USERNANE: str
    DB_PASSWORD: str
    DB_NAME: str
    
    class Config:
        env_file='.env'
        env_file_encoding='utf-8'
        extra='ignore'