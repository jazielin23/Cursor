from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse, JSONResponse

import model


ROOT = Path(__file__).resolve().parent
STATIC = ROOT / "static"

app = FastAPI(title="Image Quality Grader", version="0.1.0")


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.post("/predict")
async def predict(image: UploadFile = File(...)):
    image_bytes = await image.read()
    grade, features = model.predict_grade(image_bytes)
    return JSONResponse({"grade": float(grade), "features": features})
