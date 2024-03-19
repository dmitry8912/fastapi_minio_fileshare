import datetime
import os
import jwt
from typing import Annotated

from dateutil.relativedelta import relativedelta

from fastapi import FastAPI, UploadFile, File, Form
from starlette.responses import StreamingResponse, JSONResponse

from minio_fastapi.minio_handler import MinioHandler
app = FastAPI()

minio_handler = MinioHandler(
    os.getenv('MINIO_URL'),
    os.getenv('MINIO_ACCESS_KEY'),
    os.getenv('MINIO_SECRET_KEY'),
    os.getenv('MINIO_BUCKET'),
    False
)


@app.post('/upload')
async def upload(file: Annotated[UploadFile, Form()]):
    minio_handler.upload_file(file.filename, file.file, file.size)
    return {
        "status": "uploaded",
        "name": file.filename
    }


@app.get('/list')
async def list_files():
    return minio_handler.list()


@app.get('/link/{file}')
async def link(file: str):
    obj = minio_handler.stats(file)
    payload = {
        "filename": obj.object_name,
        "valid_til": str(datetime.datetime.utcnow() + relativedelta(minutes=int(os.getenv('LINK_VALID_MINUTES', 10))))
    }
    encoded_jwt = jwt.encode(payload, os.getenv('JWT_SECRET'), algorithm="HS256")

    return {
        "link": f"/download/{encoded_jwt}"
    }


@app.get('/download/{temp_link}')
async def download(temp_link: str):
    try:
        decoded_jwt = jwt.decode(temp_link, os.getenv('JWT_SECRET'), algorithms=["HS256"])
    except:
        return JSONResponse({
            "status": "failed",
            "reason": "Link expired or invalid"
        }, status_code=400)

    valid_til = datetime.datetime.strptime(decoded_jwt['valid_til'], '%Y-%m-%d %H:%M:%S.%f')
    if valid_til > datetime.datetime.utcnow():
        filename = decoded_jwt['filename']
        return StreamingResponse(
            minio_handler.download_file(filename),
            media_type='application/octet-stream'
        )
    return JSONResponse({
        "status": "failed",
        "reason": "Link expired or invalid"
    }, status_code=400)
