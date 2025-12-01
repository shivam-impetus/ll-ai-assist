import streamlit as st
import subprocess
import sys
import socket

port = 8201

def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

# Start API as a separate process only once
if "api_started" not in st.session_state:
    if not is_port_in_use(port):
        subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "api_server:api", "--host", "0.0.0.0", "--port", str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    st.session_state.api_started = True

st.title("Streamlit with Embedded REST API")
st.write(f"API running on: http://localhost:{port}")
st.write(f"Try: http://localhost:{port}/api/hello")
