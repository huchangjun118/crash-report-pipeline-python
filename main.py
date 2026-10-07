import os

import uvicorn

from crash_pipeline.server.app import create_app
from crash_pipeline.server.storage import EventStore

DB_PATH = os.environ.get("CRASH_PIPELINE_DB", "pipeline.db")

app = create_app(EventStore(DB_PATH))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
