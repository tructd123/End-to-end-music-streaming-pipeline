import os

import pytest
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_google_genai import ChatGoogleGenerativeAI

load_dotenv()


def test_gemini_api_connection():
    api_key = os.getenv("GEMINI_API_KEY")
    assert api_key, "GEMINI_API_KEY must be set in environment"

    llm_model = os.getenv("LLM_MODEL", "gemini-2.5-flash")

    try:
        llm = ChatGoogleGenerativeAI(model=llm_model, google_api_key=api_key, temperature=0.0)
        response = llm.invoke([HumanMessage(content="Return exactly one word: OK")])
        assert response.content is not None
        assert len(response.content) > 0
    except Exception as e:
        pytest.fail(f"Connection to Gemini API failed: {e}")
