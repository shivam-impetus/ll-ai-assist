import streamlit as st
from fastapi import FastAPI
from threading import Thread
import uvicorn

api = FastAPI()


@api.get("/api/hello")
def hello():
    st.write('hello called')
    return {"message": "Hello from Streamlit API!"}


def start_api():
    uvicorn.run("app:api", host="0.0.0.0", port=8201, reload=True)


if __name__ == "__main__":
    Thread(target=start_api, daemon=True).start()
    st.title("Streamlit with Embedded REST API")
    st.write("API running on :8000")
