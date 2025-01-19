from fastapi import FastAPI
from fastapi.websockets import WebSocket, WebSocketDisconnect
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from starlette.responses import FileResponse
from dotenv import load_dotenv
from ELTProcess.dataProcess import DataProcess
import os
import json

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


@app.websocket("/ws")
async def loadData(ws: WebSocket):
    try:
        await ws.accept()
        while True:
            data = await ws.receive_text()
            receiveData = json.loads(data)
            search_querys = receiveData["query"]
            year_data = receiveData["year"]
            if search_querys is not None:
                load_dotenv()
                get_url = os.getenv("OPENAPI_URL")
                openkeys = os.getenv("OPENAPI_KEY")
                openUrl = (
                    f"{get_url}{openkeys}/xml/tbLnOpendataRtmsV/1/1000/{year_data}"
                )
                elt_process = DataProcess(openUrl, year_data, search_querys)
                result_http = await elt_process.process_req()
                if result_http:
                    # Snowflake 스토리지와 DB 저장
                    elt_process.process_Data(result_http)
                    ml_json = elt_process.realEateModel()
                    # print(ml_json)
                    await ws.send_text(ml_json)
            else:
                load_dotenv()
                get_url = os.getenv("OPENAPI_URL")
                openkeys = os.getenv("OPENAPI_KEY")
                openUrl = (
                    f"{get_url}{openkeys}/xml/tbLnOpendataRtmsV/1/1000/{year_data}"
                )
                elt_process = DataProcess(openUrl, year_data, None)
                result_http = await elt_process.process_req()
                if result_http:
                    # Snowflake 스토리지와 DB 저장
                    elt_process.process_Data(result_http)
                    ml_json = elt_process.realEateModel()
                    # print(ml_json)
                    await ws.send_text(ml_json)
    except WebSocketDisconnect:
        print("연결 종료")
        try:
            await ws.close()
        except RuntimeError:
            print("ws close")


@app.get("/main")
async def mainPage():
    return FileResponse("frontend/dist/index.html")
