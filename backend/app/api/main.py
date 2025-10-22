from fastapi import FastAPI
from .routes import search, vin, scrape, analytics, uploads

app = FastAPI(title="VIN Intelligence API", version="0.1.0")

app.include_router(search.router, prefix="/search", tags=["search"])
app.include_router(vin.router, prefix="/vin", tags=["vin"])
app.include_router(scrape.router, prefix="/scrape", tags=["scrape"])
app.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
app.include_router(uploads.router, prefix="/uploads", tags=["uploads"])
