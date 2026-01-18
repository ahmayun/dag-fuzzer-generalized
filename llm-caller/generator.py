#!/usr/bin/env python3

import os
import sys
import argparse
import json
from pathlib import Path
from typing import Optional

import anthropic
from dotenv import load_dotenv

load_dotenv()


class RandomFunctionGenerator:
    def __init__(self, claude_model: str = "claude-3-7-sonnet-20250219"):
        self.claude_model = claude_model

        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in environment")

        self.client = anthropic.Anthropic(api_key=api_key)

    def generate(self, prompt: str) -> str:
        response = self.client.messages.create(
            model=self.claude_model,
            messages=[{
                "role": "user",
                "content": [{"type": "text", "text": prompt}]
            }],
            max_tokens=4096,
            temperature=0.8
        )

        content = ""
        for item in response.content:
            if hasattr(item, "text"):
                content += item.text

        return content.strip()

def strip_code_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        return "\n".join(lines).strip()
    return text

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--prompt",
        type=str,
        default="""Generate a json file of the following format
```
{ "functions": [] }
```
The functions array should contain 3 Python functions. Each function should be short and simple that does something arbitrary.
Each function should be:
- Complete and runnable
- Contain only code (no comments or docstrings)
- Take a single argument that can be any primitive type
- Should be between 1-10 lines of code"""
    )
    parser.add_argument(
        "--out",
        type=str,
        required=True,
        help="Full output file path (directories will be created)"
    )
    parser.add_argument(
        "--claude-model",
        type=str,
        default="claude-3-7-sonnet-20250219"
    )

    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    generator = RandomFunctionGenerator(args.claude_model)

    raw_output = generator.generate(args.prompt)
    clean_output = strip_code_fences(raw_output)

    try:
        parsed = json.loads(clean_output)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            "LLM did not return valid JSON. Raw output:\n" + raw_output
        ) from e

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2)

    return 0


if __name__ == "__main__":
    sys.exit(main())
