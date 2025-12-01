from abc import ABC, abstractmethod
from typing import Union, List

class BaseRAGSystem(ABC):
    @abstractmethod
    def get_model_name(self) -> str:
        """Return the name of the model being used by the RAG system"""
        pass
    
    @abstractmethod
    def reload_knowledge_base(self):
        """Reload the knowledge base"""
        pass
    
    @abstractmethod
    def answer_question(self, question: str, file_filter: Union[str, List[str]] = None, conversation_history: list = None) -> dict:
        """
        Answer a user question using the RAG pipeline
        """
        pass
    
    @abstractmethod
    def get_statistics(self) -> dict:
        """Return statistics about the RAG system"""
        return {
            "model_name": self.get_model_name(),
            # Additional statistics can be added here
        }