from abc import ABC, abstractmethod
from enum import Enum

class RAGSystemType(Enum):
    GEMMA = 'gemma'
    OPENAI = 'openai'


class RAGSystemFactory(ABC):
    @staticmethod
    def get_rag_system(system_type: RAGSystemType, **kwargs):
        """
        Factory method to create RAG system instances based on the specified type.
        
        Args:
            system_type (RAGSystemType): Type of RAG system to create (e.g., RAGSystemType.GEMMA).
            **kwargs: Additional parameters for RAG system initialization.
        
        Returns:
            BaseRAGSystem: An instance of a RAG system.
        """
        if system_type.value == 'gemma':
            from rag_systems.gemma_rag_system import GemmaRAGSystem
            return GemmaRAGSystem(**kwargs)
        elif system_type.value == 'openai':
            from rag_systems.openai_rag_system import OpenAIRAGSystem
            return OpenAIRAGSystem(**kwargs)  # Placeholder for OpenAI RAG system implementation
        else:
            raise ValueError(f"Unknown RAG system type: {system_type}")