import asyncio
import os
import signal
import subprocess
import sys
import pandas as pd
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from typing import Optional
import pandas as pd
import os

# Global variable to store the subprocess reference
watch_process: Optional[subprocess.Popen] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start the fluctuation watch
    global watch_process
    try:
        watch_process = subprocess.Popen(
            [sys.executable, "fluctuation.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"Started fluctuation watch with PID: {watch_process.pid}")
        yield
    finally:
        # Shutdown: Stop the fluctuation watch
        if watch_process:
            watch_process.terminate()
            try:
                watch_process.wait(timeout=5)
                print("Fluctuation watch stopped gracefully")
            except subprocess.TimeoutExpired:
                watch_process.kill()
                print("Fluctuation watch was force stopped")

app = FastAPI(lifespan=lifespan)

# Mount static files directory
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Create directories if they don't exist
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)

# HTML template is now in templates/changes_by_concept.html

@app.get("/api/changes/csv")
async def get_changes_csv():
    """Get changes data in CSV format"""
    csv_path = "static/changes.csv"
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="CSV file not found")
    return FileResponse(csv_path, media_type="text/csv", filename="changes.csv")

@app.get("/api/changes/json")
async def get_changes_json():
    """Get changes data in JSON format"""
    csv_path = "static/changes.csv"
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="CSV file not found")
    
    try:
        # Read CSV and fill NaN values with None
        df = pd.read_csv(csv_path)
        # Convert DataFrame to list of dicts, replacing NaN with None
        data = df.where(pd.notnull(df), None).to_dict(orient="records")
        return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV: {str(e)}")

@app.get("/changes_by_concept", response_class=HTMLResponse)
async def get_changes_by_concept(request: Request):
    return templates.TemplateResponse("changes_by_concept.html", {"request": request})

@app.get("/api/watch/status")
async def get_watch_status():
    """Get the status of the fluctuation watch process"""
    global watch_process
    if watch_process is None:
        return {"status": "not_running"}
    
    return_code = watch_process.poll()
    if return_code is None:
        return {"status": "running", "pid": watch_process.pid}
    else:
        return {"status": "stopped", "return_code": return_code}

@app.post("/api/watch/restart")
async def restart_watch():
    """Restart the fluctuation watch process"""
    global watch_process
    
    # Stop existing process if running
    if watch_process:
        watch_process.terminate()
        try:
            watch_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            watch_process.kill()
    
    # Start new process
    watch_process = subprocess.Popen(
        [sys.executable, "fluctuation.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    return {"status": "restarted", "pid": watch_process.pid}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
