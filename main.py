import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from uuid import uuid4
import asyncio
from threading import Thread

from report_generator import generate_report, get_report_status, get_report_filepath 

app = FastAPI()

# In-memory storage for task status and reports
task_status = {}


@app.post("/trigger_report")
async def trigger_report():
    report_id = str(uuid4())

    task_status[report_id] = "Running"

    loop = asyncio.get_event_loop()
    thread = Thread(target=generate_report, args=(report_id, task_status))
    thread.start()

    return JSONResponse(content={"report_id": report_id})


@app.get("/get_report/{report_id}")
async def get_report(report_id: str):
    if report_id not in task_status:
        raise HTTPException(status_code=404, detail="Report not found")

    status = get_report_status(report_id, task_status)

    if status == "Complete":
        filepath = get_report_filepath(report_id)
        return FileResponse(filepath, media_type="text/csv", filename=f"{report_id}.csv")
    else:
        return JSONResponse(content={"status": status})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
