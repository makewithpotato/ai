# app/main.py
import os
from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from app.routers import chat, transcribe, scene

load_dotenv()

app = FastAPI()

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

app.include_router(chat.router)
app.include_router(transcribe.router)
app.include_router(scene.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to DWP API"}
