import streamlit as st
from fastapi import FastAPI
import uvicorn

api = FastAPI()

@api.get("/api/hello")
def hello():
    return {"message": "Hello from Streamlit API!"}


if __name__ == "__main__":
    uvicorn.run("app:api", host="0.0.0.0", port=8101, reload=True)
    st.title("Streamlit with Embedded REST API")
    st.write("API running on :8000")
