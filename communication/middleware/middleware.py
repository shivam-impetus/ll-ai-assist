import os
from core.config.config import RAG_MODEL, CONVERSION_RAG_MODEL
from core.models.rag_system_factory import RAGSystemFactory


class Middleware:
    """Middleware layer to interface with the RAG system"""
    
    def __init__(self):
        self.rag_system = RAGSystemFactory.get_rag_system(RAG_MODEL)
        self.conversion_rag_system = RAGSystemFactory.get_rag_system(CONVERSION_RAG_MODEL)

    def answer_question(self,question: str, file_filter=None, conversation_history=None) -> dict:
        """
        Answer a user question using the RAG pipeline

        Args:
            question (str): The user's question
            file_filter (str | list, optional): Filter for specific files or categories
            conversation_history (list, optional): Previous conversation history

        Returns:
            dict: Response containing answer, sources, and metadata
        """
        print(file_filter)
        return self.rag_system.answer_question(question, file_filter, conversation_history)
    
    def get_model_name(self):
        """Return the name of the model being used"""
        return self.rag_system.get_model_name()

    def reload_knowledge_base(self):
        """Reload the knowledge base"""
        self.rag_system.reload_knowledge_base()
        
    def get_statistics(self):
        """Return statistics about the RAG system"""
        return self.rag_system.get_statistics()

    def convert_code(self, question: str, conversation_history=None) -> dict:
        """
        Convert code using the conversion assistant RAG system

        Args:
            question (str): The user's question
            conversation_history (list, optional): Previous conversation history

        Returns:
            dict: Response containing answer, sources, and metadata
        """
        return self.conversion_rag_system.convert_code(question, conversation_history)
    
    @staticmethod
    def login(self, login_id: str, login_password: str) -> dict:
        """Handle user login by verifying credentials"""
        login_id= os.getenv("LOGIN_ID")
        login_password = os.getenv("LOGIN_PASSWORD")
        if login_id == os.getenv("LOGIN_ID") and login_password == os.getenv("LOGIN_PASSWORD"):
            return {"message": "Login successful"}
        else:
            return {"error": "Invalid login credentials"}