from fastapi import APIRouter, UploadFile, File
router = APIRouter()

@router.post("")
async def upload(file: UploadFile = File(...)):
    # TODO: parse, map columns, write observations(source='upload')
    return {"filename": file.filename, "rows_ingested": 0, "rows_updated": 0, "errors": []}
