"""
Central configuration for cjeu-py.
All paths, API endpoints, rate limits, and model settings.
"""
import os

# ── API Keys ──────────────────────────────────────────────────────────────────
# Read from env var first, then fall back to file
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    _key_file = os.path.join(os.path.dirname(__file__), "..", "..", "Resources", "gemini_api_key.txt")
    if os.path.exists(_key_file):
        GEMINI_API_KEY = open(_key_file).read().strip()

# ── Models ────────────────────────────────────────────────────────────────────
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_MODEL_PRO = "gemini-2.5-pro"  # for difficult/ambiguous cases

# ── OpenAI-compatible provider (Ollama, vLLM, llama.cpp, LM Studio) ──────
OPENAI_API_BASE = os.environ.get("OPENAI_API_BASE", "http://localhost:11434/v1")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "ollama")  # Ollama doesn't need a real key
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gemma2")

# ── CELLAR / EUR-Lex endpoints ────────────────────────────────────────────────
CELLAR_SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
EURLEX_HTML_BASE = "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:"
EURLEX_REST_BASE = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:"

# ── Rate limits ───────────────────────────────────────────────────────────────
EURLEX_RATE_LIMIT = 2.0       # seconds between EUR-Lex requests
CELLAR_RATE_LIMIT = 0.5       # seconds between SPARQL queries
GEMINI_MAX_WORKERS = 5        # concurrent Gemini API calls (safe for free tier)
GEMINI_SUBMIT_DELAY = 0.05    # seconds between submissions

# ── Data paths ────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")

RAW_CELLAR_DIR = os.path.join(DATA_ROOT, "raw", "cellar")
RAW_TEXTS_DIR = os.path.join(DATA_ROOT, "raw", "texts")
PROCESSED_DIR = os.path.join(DATA_ROOT, "processed")
CLASSIFIED_DIR = os.path.join(DATA_ROOT, "classified")

# ── Gemini pricing (Flash, as of Feb 2026) ────────────────────────────────────
PRICE_PER_M_INPUT = 0.15    # $/1M input tokens
PRICE_PER_M_OUTPUT = 0.60   # $/1M output tokens
PRICE_PER_M_THINKING = 3.50 # $/1M thinking tokens (hidden CoT in 2.5 models)

# ── SPARQL pagination ─────────────────────────────────────────────────────────
SPARQL_BATCH_SIZE = 10000    # OFFSET/LIMIT batch for large queries
