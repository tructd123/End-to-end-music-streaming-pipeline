"""Minimal Gemini API diagnostic."""
import sys, os
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY", "")
print(f"API Key prefix: {API_KEY[:12]}...")

# Test 1: Try models/gemini-2.0-flash (full path)
print("\n--- Test A: model='models/gemini-2.0-flash' ---")
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.messages import HumanMessage
    llm = ChatGoogleGenerativeAI(model="models/gemini-2.0-flash", google_api_key=API_KEY, temperature=0.7)
    r = llm.invoke([HumanMessage(content="Say hi")])
    print(f"  OK: {r.content[:100]}")
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {str(e)[:300]}")

# Test 2: Try gemini-2.0-flash (short)
print("\n--- Test B: model='gemini-2.0-flash' ---")
try:
    llm2 = ChatGoogleGenerativeAI(model="gemini-2.0-flash", google_api_key=API_KEY, temperature=0.7)
    r2 = llm2.invoke([HumanMessage(content="Say hi")])
    print(f"  OK: {r2.content[:100]}")
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {str(e)[:300]}")

# Test 3: Try gemini-1.5-flash
print("\n--- Test C: model='gemini-1.5-flash' ---")
try:
    llm3 = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=API_KEY, temperature=0.7)
    r3 = llm3.invoke([HumanMessage(content="Say hi")])
    print(f"  OK: {r3.content[:100]}")
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {str(e)[:300]}")

# Test 4: Embedding
print("\n--- Test D: Embedding 'models/text-embedding-004' ---")
try:
    from langchain_google_genai import GoogleGenerativeAIEmbeddings
    emb = GoogleGenerativeAIEmbeddings(model="models/text-embedding-004", google_api_key=API_KEY)
    vec = emb.embed_query("hello world")
    print(f"  OK: dim={len(vec)}")
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {str(e)[:300]}")

# Test 5: Embedding with models/embedding-001
print("\n--- Test E: Embedding 'models/embedding-001' ---")
try:
    emb2 = GoogleGenerativeAIEmbeddings(model="models/embedding-001", google_api_key=API_KEY)
    vec2 = emb2.embed_query("hello world")
    print(f"  OK: dim={len(vec2)}")
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {str(e)[:300]}")

print("\nDone.")
