from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, List, Union
from communication.middleware.middleware import Middleware
import logging, datetime
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
api = FastAPI()
middleware_instance = Middleware()

api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QuestionRequest(BaseModel):
    question: str
    file_filter: Optional[Union[str, List[str]]] = None
    conversation_history: Optional[List[dict]] = None

class LoginRequest(BaseModel):
    login_id: str
    login_password: str

@api.post("/login")
async def login(request: LoginRequest):
    """Handle user login by verifying credentials"""
    try:
        result = middleware_instance.login(request.login_id, request.login_password)
        return result
    except Exception as e:
        return {"error": str(e)}

@api.post("/generate-answer")
async def answer_question(request: QuestionRequest):
    """
    Answer a question using the RAG system
    
    Args:
        String question, optional file_filter, and optional conversation_history
        
    Returns:
        dict: Response containing answer, sources, and metadata
    """
    try:
        result = middleware_instance.answer_question(request.question,
                                            request.file_filter,
                                            request.conversation_history
                                            )
        return result
    except Exception as e:
        return {"error": str(e), "answer": "", "sources": []}

@api.get("/")
async def root():
    """Root endpoint to check if the API is running"""
    return {"message": "LeapLogic RAG API is running", "version": "1.0.0"}

@api.get("/get-statistics")
async def get_statistics():
    """Health check endpoint"""
    try:
        result = middleware_instance.get_statistics()
        return result
    except Exception as e:
        return {"error": str(e), "answer": "", "sources": []}

@api.get("/get-model-name")
async def get_model_name():
    """Endpoint to get the name of the model being used"""
    try:
        model_name = middleware_instance.get_model_name()
        return {"model_name": model_name}
    except Exception as e:
        return {"error": str(e), "model_name": ""}
    
    
@api.post("/reload-knowledge-base")
async def reload_knowledge_base():
    """Endpoint to reload the knowledge base"""
    try:
        middleware_instance.reload_knowledge_base()
        return {"message": "Knowledge base reloaded successfully"}
    except Exception as e:
        return {"error": str(e)}
    
@api.post("/convert-code")
async def convert_code(request: QuestionRequest):
    """
    Convert code using the conversion assistant RAG system
    
    Args:
        String question, optional file_filter, and optional conversation_history
        
    Returns:
        dict: Response containing converted code, sources, and metadata
    """
    try:
        result = middleware_instance.convert_code(
            request.question,
            request.conversation_history
        )
        return result
    except Exception as e:
        return {"error": str(e), "answer": "", "sources": []}

    
@api.middleware("http")
async def log_requests(request, call_next):
    start_time = datetime.datetime.now()    
    client_ip = request.client.host
    method = request.method
    path = request.url.path
    logging.info(f"Incoming request: {method} {path} from {client_ip} at {start_time}")
    
    response = await call_next(request)
    
    process_time = (datetime.datetime.now() - start_time).total_seconds()
    logging.info(f"Completed {method} {path} in {process_time}s with status {response.status_code}")
    return response