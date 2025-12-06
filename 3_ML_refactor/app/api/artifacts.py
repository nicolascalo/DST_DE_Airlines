from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
import glob
import re
import shutil

router = APIRouter(tags=["training_outputs"])


@router.get("/display_training_run_list")
def list_training_logs():
    """Display the list of training runs"""
    try:
        log_list = glob.glob("./outputs/**/*/*.log", recursive=True)
        log_list.sort(reverse=True)
        log_list = [re.sub(".*/", "", f) for f in log_list]
        log_list = dict(enumerate(log_list))
        return JSONResponse(log_list)
    except:
        raise HTTPException(status_code=404, detail="No logs found")


@router.get("/display_last_training_log", response_class=PlainTextResponse)
def display_last_training_log():
    """Display the log of the last training"""
    try:
        log_list = glob.glob("./outputs/**/*/*.log", recursive=True)
        log_list.sort(reverse=True)
        with open(log_list[0]) as f:
            return f.read()
    except:
        raise HTTPException(status_code=404, detail="No log found")


@router.get("/download_best_models")
def download_best_models():
    """Downloads the best overall models."""
    try:
        file_name = "afklm_ml_best_models"
        file_path = "./outputs/best_models"

        shutil.make_archive(f"./outputs/{file_name}", "zip", file_path)

        return FileResponse(
            path=f"./outputs/{file_name}.zip",
            filename=f"{file_name}.zip",
            media_type="application/zip"
        )
    except:
        raise HTTPException(status_code=404, detail="No models found")
