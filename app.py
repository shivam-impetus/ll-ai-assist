import streamlit as st
from fastapi import FastAPI
from threading import Thread
import uvicorn
import socket
import asyncio

api = FastAPI()
port = 8201

@api.get("/api/hello")
def hello():
    return {"message": "Hello from Streamlit API!"}


def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def start_api():
    # Create a new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    config = uvicorn.Config(api, host="0.0.0.0", port=port, log_level="info", loop="asyncio")
    server = uvicorn.Server(config)
    
    # Disable signal handlers since we're not in the main thread
    server.install_signal_handlers = lambda: None
    
    loop.run_until_complete(server.serve())


# Start API in background thread only once
if "api_started" not in st.session_state:
    st.session_state.api_started = True
    if not is_port_in_use(port):
        thread = Thread(target=start_api, daemon=True)
        thread.start()

st.title("Streamlit with Embedded REST API")
st.write(f"API running on: http://localhost:{port}")
st.write(f"Try: http://localhost:{port}/api/hello")
