from pydantic import BaseModel
import os

class Settings(BaseModel):
    database_url: str = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/vinintel")
    object_store_url: str = os.getenv("OBJECT_STORE_URL", "http://localhost:9000")
    object_store_bucket: str = os.getenv("OBJECT_STORE_BUCKET", "vin-raw")
    firecrawl_api_key: str | None = os.getenv("FIRECRAWL_API_KEY")

settings = Settings()
