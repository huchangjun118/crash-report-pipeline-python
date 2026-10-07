"""启动服务端：uvicorn main:app --host 127.0.0.1 --port 8000"""
import os

from crash_pipeline.server.app import create_app

app = create_app(os.environ.get("CRASH_PIPELINE_DB", "crash_pipeline.db"))

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
