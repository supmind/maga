from fastapi import FastAPI
from .database import create_db_and_tables

app = FastAPI(title="Distributed Screenshot Service")

@app.on_event("startup")
def on_startup():
    # This function is called when the application starts.
    # It will create the database and all the necessary tables.
    create_db_and_tables()

@app.get("/", summary="Health Check")
def read_root():
    """A simple health check endpoint to confirm the server is running."""
    return {"status": "ok", "message": "Welcome to the Distributed Screenshot Service!"}

# We will include the API routers here.
from .api import workers
app.include_router(workers.router)

from .api import tasks
app.include_router(tasks.router)
