import streamlit as st
import json

# Check if this is an API request via query params
query_params = st.query_params

if query_params.get("api") == "hello":
    # Return JSON response for API calls
    # Use st.json for display or write raw response
    response = {"message": "Hello from Streamlit API!"}
    st.json(response)
    st.stop()

# Regular Streamlit UI
st.title("Streamlit with Embedded REST API")
st.write("### API Endpoints:")
st.code("GET /?api=hello", language="text")
st.write("### Example Response:")
st.json({"message": "Hello from Streamlit API!"})
