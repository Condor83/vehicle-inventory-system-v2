from fastapi import APIRouter, UploadFile, File, HTTPException

from backend.app.services.upload_ingest import ingest_vehicle_locator_upload

router = APIRouter()


@router.post("")
async def upload(file: UploadFile = File(...)):
    content = await file.read()
    try:
        return ingest_vehicle_locator_upload(file.filename, content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - unexpected failure
        raise HTTPException(status_code=500, detail="Upload failed") from exc
