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
from ETLProcess.Data import DataProcess
from ETLProcess.AnalyticsML import Analytics

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


# Airflow load DAG Trigger
def trigger_airflow_dag(year_data, query, openUrl: str):
    # if logins:
    trigger_url = "https://localhost:8081/api/v1/dags/spark-process-dag/dagRuns"

    headers = {
        "Content-Type": "application/json",
    }
    payload = {"conf": {"year_data": year_data, "query": query, "openUrl": openUrl}}
    #  headers=headers
    rep = requests.post(trigger_url, json=payload, headers=headers, verify=False)
    if rep.status_code == 200:
        print(rep.text)
        print("airflow success")
    else:
        print(rep.text)
        print("airflow falild")


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

            trigger_airflow_dag(year_data, build_name, openUrl)
            # dp = DataProcess(year_data, openUrl)
            # process_data = await dp.fetchData()
            # outpath = dp.save_to_parequst(process_data)

            # await websocket.send(ml_json)
            # trigger_airflow_dag(year_data, build_name, )
    except Exception as e:
        print(e)


async def start_ws_connect():

    async with serve(conenctWS, "localhost", 8001):
        await asyncio.Future()


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
