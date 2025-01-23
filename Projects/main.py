from fastapi import FastAPI
from fastapi.websockets import WebSocket, WebSocketDisconnect
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from starlette.responses import FileResponse
from dotenv import load_dotenv
import os
import json
import requests
import uvicorn.config
from websockets import serve
import asyncio
import uvicorn
from airflow.models import Variable

# BackEnd WebServer 구성

app = FastAPI()

origins = [
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

frontend_dir = os.path.abspath("frontend/dist/assets")

app.mount("/assets", StaticFiles(directory=frontend_dir))


@app.get("/")
async def mainPage():
    return FileResponse("frontend/dist/index.html")


async def conenctWS(websocket):
    try:
        async for message in websocket:
            load_dotenv()
            recevie_msg = json.loads(message)
            build_name = recevie_msg["query"]
            year_data = recevie_msg["year"]
            get_url = os.getenv("OPENAPI_URL")
            openkeys = os.getenv("OPENAPI_KEY")
            openUrl = f"{get_url}{openkeys}/xml/tbLnOpendataRtmsV/1/1000/{year_data}"
            if build_name is not None:
                Variable.set("build_names", build_name)
            else:
                # airflow model 에 저장한다.
                Variable.set("years", year_data)
                Variable.set("open_url", openUrl)

                ml_json = Variable.get("ml_json")
                if ml_json is not None:
                    await websocket.send(ml_json)
    except Exception as e:
        print(e)


# WebSocket connect
async def start_ws_connect():

    async with serve(conenctWS, "localhost", 8001):
        await asyncio.Future()


# FASTAPI 호출
async def start_webFast():
    config = uvicorn.Config(app, host="localhost", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    websever_task = asyncio.create_task(start_ws_connect())
    fast_task = asyncio.create_task(start_webFast())
    await asyncio.gather(websever_task, fast_task)


if __name__ == "__main__":
    asyncio.run(main())
