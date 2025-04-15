from typing import Union
from fastapi import FastAPI, HTTPException
from src.generate_report import generate_report
from fastapi.responses import FileResponse
import threading
import uuid
import pandas as pd
from src.config import REPORT_DIR
import os

app = FastAPI()
report_Status = {}
report_locks = {}
report_errors = {}

@app.get("/")
def read_root(): 
    return {"Hello":"World"}

def run_gen_report_job(report_id):
    try:
        df = generate_report()
        report_path = os.path.join(REPORT_DIR, f"Report_{report_id}.csv")
        df.to_csv(report_path, index = False)
        report_Status[report_id] = "Complete"
    except Exception as e:
        report_Status[report_id] = "Failed"
        report_errors[report_id] = str(e)
    finally:
        report_locks[report_id].release()

@app.post("/triger_report")
def trigger_report():
    report_id = str(uuid.uuid4())
    report_Status[report_id] = "Running"
    report_locks[report_id] = threading.Semaphore(0)

    thread = threading.Thread(target = run_gen_report_job, args = (report_id,))
    thread.start()
    return {"Report_id" : report_id}

@app.get("/get_report")
def get_report(report_id : str):
    if report_id not in report_Status:
        raise HTTPException(status_code = 404, detail = "Invalid report_id provided." )
    status = report_Status[report_id]
    if status == "Running":
        return {"status" : "Running"}
    elif status == "Failed":
        return {"status" : "Failed", "Error" : report_errors.get(report_id, "Unknown Error")}
    report_path = os.path.join(REPORT_DIR, f"Report_{report_id}.csv")
    # print(report_path)
    if not os.path.exists(report_path):
        raise HTTPException(status_code=500, detail=f"Report marked complete, but file missing ${report_path}")

    return FileResponse(report_path, media_type="text/csv", filename=f"Report_{report_id}.csv")