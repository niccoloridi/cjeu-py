"""
Gemini LLM client — wrapper for google-genai SDK.
Provides structured JSON generation for citation classification.
Includes exponential backoff retry on rate-limit (429) errors.
"""
import json
import logging
import time
from google import genai
from google.genai import types
from cjeu_py import config

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
INITIAL_BACKOFF = 2  # seconds


def get_gemini_client() -> genai.Client:
    """Initialise and return the Gemini client."""
    if not config.GEMINI_API_KEY:
        raise ValueError(
            "GEMINI_API_KEY not set. Either set the environment variable "
            "or place your key in Resources/gemini_api_key.txt"
        )
    return genai.Client(api_key=config.GEMINI_API_KEY)


def classify_citation(
    client: genai.Client,
    prompt: str,
    response_schema: dict,
    model: str = None,
) -> dict:
    """
    Send a classification prompt to Gemini and return structured JSON.
    Retries with exponential backoff on 429/RESOURCE_EXHAUSTED errors.

    Args:
        client: Gemini client instance
        prompt: The full classification prompt with context
        response_schema: JSON schema for the expected response
        model: Model name (defaults to config.GEMINI_MODEL)

    Returns:
        dict with classification results + _meta with token counts
    """
    model = model or config.GEMINI_MODEL

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=response_schema,
                    thinking_config=types.ThinkingConfig(
                        thinking_budget=0,
                    ),
                ),
            )

            result = json.loads(response.text)

            usage = getattr(response, "usage_metadata", None)
            thinking_tokens = getattr(usage, "thoughts_token_count", 0) if usage else 0
            result["_meta"] = {
                "model": model,
                "input_tokens": getattr(usage, "prompt_token_count", 0) if usage else 0,
                "output_tokens": getattr(usage, "candidates_token_count", 0) if usage else 0,
                "thinking_tokens": thinking_tokens or 0,
                "error": None,
            }

            return result

        except Exception as e:
            error_str = str(e)
            # Retry on rate limit errors with exponential backoff
            if ("429" in error_str or "RESOURCE_EXHAUSTED" in error_str) and attempt < MAX_RETRIES:
                wait = INITIAL_BACKOFF * (2 ** attempt)  # 2, 4, 8 seconds
                logger.warning(
                    f"Rate limited (attempt {attempt + 1}/{MAX_RETRIES + 1}), "
                    f"waiting {wait}s..."
                )
                time.sleep(wait)
                continue

            # Non-retryable error or max retries exceeded
            logger.error(f"Gemini API error: {e}")
            return {
                "_meta": {
                    "model": model,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "error": error_str,
                }
            }


def count_tokens(client: genai.Client, text: str, model: str = None) -> int:
    """Count tokens for a given text using the model."""
    model = model or config.GEMINI_MODEL
    try:
        response = client.models.count_tokens(model=model, contents=text)
        return response.total_tokens
    except Exception as e:
        logger.warning(f"Failed to count tokens: {e}")
        return 0
