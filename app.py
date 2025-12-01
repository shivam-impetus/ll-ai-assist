import streamlit as st
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from threading import Thread
import uvicorn
import socket
import asyncio
import time

api = FastAPI()

# Add CORS middleware to allow requests from any origin
api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        time.sleep(1)  # Give the server time to start

api_running = is_port_in_use(port)

st.title("Streamlit with Embedded REST API")
st.write(f"API Status: {'✅ Running' if api_running else '❌ Not Running'}")
st.write(f"Local: http://localhost:{port}/api/hello")
st.write(f"Network: http://192.168.1.3:{port}/api/hello")
