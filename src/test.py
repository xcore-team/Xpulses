from xcore import Xcore
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager



xcore = Xcore("./integration.yaml")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await xcore.boot()
    yield
    await xcore.shutdown()




app = FastAPI(lifespan=lifespan)