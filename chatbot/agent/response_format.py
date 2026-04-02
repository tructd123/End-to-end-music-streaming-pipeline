"""Utilities to normalize chatbot text for readability in the UI."""

import re


def normalize_response_text(text: str) -> str:
    """Normalize model output into a readable multiline format.

    This is a light post-processing step for cases where the LLM returns
    numbered lists in a single long line.
    """
    if not text:
        return ""

    normalized = text.strip()

    # Remove markdown bold markers because frontend renders plain text.
    normalized = re.sub(r"\*\*(.*?)\*\*", r"\1", normalized)

    # Normalize common inline markdown list style: "* title: content"
    normalized = re.sub(r"\s*\*\s+", "\n- ", normalized)

    # Ensure list begins on a new paragraph when introduced after a sentence.
    normalized = re.sub(r":\s*(?=\d+\.\s|[-]\s)", ":\n\n", normalized)

    # Split inline numbered items like: "... 1. ... 2. ... 3. ..."
    normalized = re.sub(r"\s+(?=\d+\.\s)", "\n\n", normalized)

    # Split inline bullet items like: "... - item1 - item2"
    normalized = re.sub(r"\s+(?=-\s)", "\n", normalized)

    # Keep one space after list marker if model outputs "-Title".
    normalized = re.sub(r"\n-(\S)", r"\n- \1", normalized)

    # Add visual spacing between bullet items.
    normalized = re.sub(r"\n\s*-\s", "\n\n- ", normalized)

    # Move CTA prompts to a new paragraph when they are stuck to the last item.
    normalized = re.sub(
        r"\s+(?=(Bạn muốn|Ban muon|Bạn cần|Bạn có muốn)\b)",
        "\n\n",
        normalized,
    )

    # Keep output compact while preserving visual spacing.
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()
