from fastapi import FastAPI
import streamlit as st

api = FastAPI()

@api.get("/api/hello")
def hello():
    st.write('<h2>hello</h2>')
    return {"message": "Hello from Streamlit API!"}
