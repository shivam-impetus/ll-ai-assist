from core.models.gemma_rag_system import GemmaRAGSystem
import os
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''


# ==============================
# MAIN INTERACTIVE SESSION
# ==============================
def main():
    """Main interactive Q&A session"""
    print("\n")
    system = GemmaRAGSystem()

    print("\n" + "=" * 60)
    print("📖 Gemma RAG System Ready!")
    print("=" * 60)

    # Display statistics
    stats = system.get_statistics()
    print(f"\n📊 Knowledge Base Statistics:")
    print(f"   • Documents loaded: {stats['documents_loaded']}")
    print(f"   • Total chunks indexed: {stats['total_chunks']}")
    print(f"   • Vector database: {stats['vector_database']}")
    print(f"   • Embedding dimension: {stats['embedding_dimension']}")

    print(f"\n💬 Type your questions below (type 'exit' or 'quit' to stop)\n")
    print("-" * 60)

    session_history = []

    while True:
        try:
            question = input("\n❓ Your question: ").strip()

            if not question:
                print("⚠ Please enter a question.")
                continue

            if question.lower() in ['exit', 'quit', 'bye']:
                print("\n👋 Thank you for using Gemma RAG System!")
                break

            if question.lower() == 'stats':
                stats = system.get_statistics()
                print("\n📊 Current Statistics:")
                for key, value in stats.items():
                    print(f"   • {key}: {value}")
                continue

            if question.lower() == 'history':
                if session_history:
                    print("\n📜 Session History:")
                    for i, item in enumerate(session_history, 1):
                        print(f"\n{i}. Q: {item['question']}")
                        print(f"   A: {item['answer'][:100]}...")
                else:
                    print("⚠ No history yet.")
                continue

            # Get answer
            result = system.answer_question(question)

            # Display answer
            print(f"\n✅ Answer:")
            print(result['answer'])

            # Show sources if available
            if result['search_results']:
                print(f"\n📚 Sources:")
                for i, source in enumerate(result['search_results'], 1):
                    print(f"   {i}. {source['file']} (confidence: {source['confidence']:.2%})")

            # Store in history
            session_history.append({
                "question": question,
                "answer": result['answer'],
                "sources": result['sources_found']
            })

        except KeyboardInterrupt:
            print("\n\n👋 Session interrupted. Goodbye!")
            break
        except Exception as e:
            print(f"\n✗ Error: {e}")


if __name__ == "__main__":
    main()
