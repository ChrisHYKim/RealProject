from fastapi import FastAPI
from fastapi.websockets import WebSocket, WebSocketDisconnect
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from starlette.responses import FileResponse
from dotenv import load_dotenv
from ETLProcess.Data import Data
from ETLProcess.AnalyticsML import Analytics
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
            if year_data is not None:
                load_dotenv()
                get_url = os.getenv("OPENAPI_URL")
                openkeys = os.getenv("OPENAPI_KEY")
                openUrl = (
                    f"{get_url}{openkeys}/xml/tbLnOpendataRtmsV/1/1000/{year_data}"
                )
                # 데이터 전처리 진행
                elt_process = Data(openUrl, year_data)
                result_http = await elt_process.fetchData()
                parequest = elt_process.process_Data(result_http)
                # 분석 진행
                analtity = Analytics(parequest)
                df = analtity.loadPreq()
                df_data = analtity.vectorProc(df)
                ml_json = analtity.trainModel(df_data)
                # print(ml_json)
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
