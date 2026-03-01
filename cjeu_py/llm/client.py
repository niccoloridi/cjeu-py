"""
LLM client — wrappers for Gemini and OpenAI-compatible APIs.
Provides structured JSON generation for citation classification.
Includes exponential backoff retry on rate-limit (429) errors.
"""
import json
import logging
import time
from cjeu_py import config

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
INITIAL_BACKOFF = 2  # seconds


def _import_genai():
    """Lazy import google-genai with a helpful error message."""
    try:
        from google import genai
        from google.genai import types
        return genai, types
    except ImportError:
        raise ImportError(
            "google-genai is required for Gemini classification. "
            "Install it with: pip install 'cjeu-py[llm]'"
        )


def get_gemini_client():
    """Initialise and return the Gemini client."""
    genai, _ = _import_genai()
    if not config.GEMINI_API_KEY:
        raise ValueError(
            "GEMINI_API_KEY not set. Either set the environment variable "
            "or place your key in Resources/gemini_api_key.txt"
        )
    return genai.Client(api_key=config.GEMINI_API_KEY)


def classify_citation(
    client,
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
    _, types = _import_genai()
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


# ── OpenAI-compatible provider ────────────────────────────────────────────


def _build_schema_instruction(response_schema: dict) -> str:
    """Convert a JSON schema dict into a plain-text instruction for the system prompt."""
    props = response_schema.get("properties", {})
    required = response_schema.get("required", [])
    lines = ["You MUST respond with a single JSON object matching this schema:", "{"]
    for key, spec in props.items():
        req_mark = " (REQUIRED)" if key in required else ""
        if "enum" in spec:
            enum_str = ", ".join(f'"{v}"' for v in spec["enum"])
            lines.append(f'  "{key}": one of [{enum_str}]{req_mark}')
        elif spec.get("type") == "number":
            lines.append(f'  "{key}": <number>{req_mark}')
        else:
            desc = spec.get("description", "string")
            lines.append(f'  "{key}": <string: {desc}>{req_mark}')
    lines.append("}")
    lines.append("")
    lines.append("Return ONLY the JSON object. No markdown, no code fences, no explanation outside the JSON.")
    return "\n".join(lines)


def get_openai_client(api_base: str = None, api_key: str = None):
    """Initialise and return an OpenAI-compatible client."""
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError(
            "openai package not installed. Install with: pip install 'cjeu-py[openai-llm]'"
        )
    return OpenAI(
        base_url=api_base or config.OPENAI_API_BASE,
        api_key=api_key or config.OPENAI_API_KEY,
    )


def classify_citation_openai(
    client,
    prompt: str,
    response_schema: dict,
    model: str = None,
) -> dict:
    """
    Send a classification prompt to an OpenAI-compatible API.
    Retries on malformed JSON or missing keys.
    """
    model = model or config.OPENAI_MODEL
    schema_instruction = _build_schema_instruction(response_schema)
    required_keys = set(response_schema.get("required", []))

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": schema_instruction},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
            )

            raw_text = response.choices[0].message.content.strip()

            # Strip markdown code fences if present
            if raw_text.startswith("```"):
                lines = raw_text.split("\n")
                if lines[-1].strip() == "```":
                    raw_text = "\n".join(lines[1:-1])
                else:
                    raw_text = "\n".join(lines[1:])

            result = json.loads(raw_text)

            # Validate required keys
            missing = required_keys - set(result.keys())
            if missing and attempt < MAX_RETRIES:
                logger.warning(
                    f"Missing keys {missing} (attempt {attempt + 1}), retrying..."
                )
                continue

            usage = getattr(response, "usage", None)
            result["_meta"] = {
                "model": model,
                "provider": "openai",
                "input_tokens": getattr(usage, "prompt_tokens", 0) if usage else 0,
                "output_tokens": getattr(usage, "completion_tokens", 0) if usage else 0,
                "thinking_tokens": 0,
                "error": None,
            }
            return result

        except json.JSONDecodeError as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"Malformed JSON (attempt {attempt + 1}): {e}")
                continue
            logger.error(f"Failed to parse JSON after {MAX_RETRIES + 1} attempts")
            return {
                "_meta": {
                    "model": model, "provider": "openai",
                    "input_tokens": 0, "output_tokens": 0,
                    "error": f"JSON parse failure: {e}",
                }
            }

        except Exception as e:
            error_str = str(e)
            if ("429" in error_str or "rate" in error_str.lower()) and attempt < MAX_RETRIES:
                wait = INITIAL_BACKOFF * (2 ** attempt)
                logger.warning(
                    f"Rate limited (attempt {attempt + 1}), waiting {wait}s..."
                )
                time.sleep(wait)
                continue
            logger.error(f"OpenAI-compatible API error: {e}")
            return {
                "_meta": {
                    "model": model, "provider": "openai",
                    "input_tokens": 0, "output_tokens": 0,
                    "error": error_str,
                }
            }


def count_tokens(client, text: str, model: str = None) -> int:
    """Count tokens for a given text using the Gemini model."""
    model = model or config.GEMINI_MODEL
    try:
        response = client.models.count_tokens(model=model, contents=text)
        return response.total_tokens
    except Exception as e:
        logger.warning(f"Failed to count tokens: {e}")
        return 0
