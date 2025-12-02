from openai import OpenAI
import streamlit as st
from core.config import config
from core.models.base_rag_system import BaseRAGSystem


class OpenAIRAGSystem(BaseRAGSystem):
    """Shell to Glue conversion assistant using OpenAI."""
    
   
    CONVERSION_SYSTEM_PROMPT = """You're a data engineer working in leaplogic team. you need to translate shell scripting logic to glue based script in python.
    Below are the things you need to take care.
    1. Create python class in upper case with same name as script name.
    2. Class should extend GlueELTStep.
    3. Min logic should be implemented in executeFlow function.
    4. Below is the structure for translated script.

    ```
    from com.impetus.idw.wmg.elt.glue_elt_step import GlueELTStep
    \
    class <SCRIPT_NAME>(GlueELTStep):

        def executeFlow(self, executor, *args, **kwargs):
            # TODO: Main logic converted in python
    \
    if __name__ == '__main__':
        step = <SCRIPT_NAME>()
        step.start()
        
    ```
    5. for executing select queries on spark use below syntax.
    ```
    query = \"\"\"
    \
    \"\"\"

    self.executor.executeQuery(query, temporary_table="temp_tbl")
    ```
    6. for executing dml queries on spark use below syntax.

    ```
    query = \"\"\"
    \
    \"\"\"
    self.executor.executeQuery(query)
    ```
    \
    7. for executing queries on other platforms use below syntax.
    ```
    query = \"\"\"
    \
    \"\"\"
    self.executor.executeQuery(query, dbType='dbms_name', secret='dbms_secret')
    ```
    \
    8. for reading csv file using spark from s3 use below syntax.
    ```
    file_path = 's3://your-bucket/path/to/your-file.csv'
    output_table = 'temp_tbl'
    options = {'header': True, 'sep': ' |'}

    self.executor.readFile(
        'csv',
        file_path,
        output_table,
        options
    )
    ```
    \
    9. for writing csv file using spark in s3 use below syntax.
    ```
    file_path = 's3://your-bucket/path/to/your-file.csv'
    input_table = 'temp_tbl'
    options = {'header': True, 'sep': ' |'}
    self.executor.writeFile(
        'csv',
        file_path,
        input_table,
        options
    )
    ```
    \
    10. for sending mail use below syntax:
    ```
    self.send_mail(
        from_email='',
        to_email=[''],
        subject='',
        body='',
        attachment_path=None
    )
    ```
    \
    11. use com folder documents to check for possible functions and use them if required.
    12. Once code is translated verify below.
        1. FUNCTIONAL CORRECTNESS REVIEW
        Compare the original and converted code line by line:
        - Are all business logic rules correctly implemented?
        - Are calculations and transformations accurate?
        - Are conditional statements and loops properly converted?
        - Are data types and precision maintained?
    \
        2. FALSE POSITIVE IDENTIFICATION
        Look for code that compiles but is functionally incorrect:
        - Assignment vs accumulation errors (= vs +=)
        - Loop logic differences or off-by-one errors
        - Incorrect variable scoping or lifecycle issues
        - Wrong data structure usage (list vs dict vs DataFrame)
    \
        3. PERFORMANCE ANTI-PATTERNS
        Identify inefficient conversion patterns:
        - Row-by-row iteration instead of vectorized operations
        - Unnecessary data copying or intermediate variables
        - Inefficient pandas operations (iterrows, append in loops)
        - Missing opportunities for SQL optimization
    \
        4. MISSING FUNCTIONALITY
        Check for incomplete conversions:
        - Error handling not implemented
        - Edge cases not covered
        - Input validation missing
        - Output formatting incomplete
    \
        5. MANUAL FIX RECOMMENDATIONS
        For each issue found, provide:
        - Exact line number and description of the problem
        - Corrected code snippet with explanation
        - Why the original conversion was incorrect
        - How to validate the fix works correctly
    \
        6. INTEGRATION CONCERNS
        Assess how this code integrates with other modules:
        - Input/output data contract compatibility
        - Shared variable or function dependencies
        - Database connection and transaction handling
        - File I/O and path management
    \
        7. TESTING STRATEGY
        Recommend specific tests needed:
        - Unit tests for critical business logic
        - Integration tests for data flow
        - Performance tests for large datasets
        - Edge case tests for boundary conditions
    """
    
    def __init__(self):
        """Initialize the conversion assistant with OpenAI client."""
        self.client = OpenAI(
            base_url="https://models.github.ai/inference",
            api_key=config.GITHUB_PAT,
            default_query={
                "api-version": "2024-08-01-preview",
            },
        )
        self.model = "openai/gpt-4.1"
    
    def convert_code(self, question, conversation_history=None, **kwargs):
        """Answer a conversion question using OpenAI.
        
        Args:
            question: The user's question about shell to Glue conversion
            conversation_history: List of previous Q&A pairs for context
            **kwargs: Additional arguments (for compatibility with GemmaRAGSystem interface)
        
        Returns:
            dict with 'answer' and 'search_results' keys
        """
        # Build messages array with system prompt and conversation history
        messages = [
            {
                "role": "system",
                "content": self.CONVERSION_SYSTEM_PROMPT,
            }
        ]
        
        # Add conversation history if provided
        if conversation_history:
            for item in conversation_history:
                messages.append({
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": item["question"],
                    }],
                })
                messages.append({
                    "role": "assistant",
                    "content": [{
                        "type": "text",
                        "text": item["answer"],
                    }],
                })
        
        # Add current question
        messages.append({
            "role": "user",
            "content": [{
                "type": "text",
                "text": question,
            }],
        })
        
        # Get response from OpenAI
        try:
            response = self.client.chat.completions.create(
                messages=messages,
                model=self.model,
                temperature=1,
                top_p=1,
            )
            
            answer = response.choices[0].message.content
            
            return {
                "answer": answer,
                "search_results": [{"file": "OpenAI GPT-4.1 Model", "confidence": 1.0}],
            }
        except Exception as e:
            return {
                "answer": f"Error calling OpenAI API: {str(e)}",
                "search_results": [],
            }
    
    def get_statistics(self):
        """Return statistics about the conversion system."""
        return {
            "documents_loaded": 0,
            "total_chunks": 0,
        }
    
    def reload_knowledge_base(self):
        """Reload is not applicable for conversion assistant."""
        pass

    def get_model_name(self):
        """Return the name of the model being used"""
        return "OpenAI GPT-4.1"
    
    def answer_question(self, question: str, file_filter=None, conversation_history=None) -> dict:
        """TODO: Implement if needed"""
        pass