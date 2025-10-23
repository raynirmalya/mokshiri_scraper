# gpt_rewriter.py
from openai import OpenAI
import json
import os
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Now fetch the key from environment
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("❌ OPENAI_API_KEY is missing. Check your .env file or environment variables.")

client = OpenAI(api_key=api_key)

REWRITE_PROMPT_TEMPLATE = """
You are an expert editor for an international lifestyle and entertainment site.

Rewrite the given title and summary to sound natural, human-like, and unique.
Rules:
- Preserve facts (names, dates, places, numbers).
- Use your own sentence structures, phrasing, and rhythm.
- Make the headline short, catchy, and SEO-friendly (≤ 12 words).
- Make the summary conversational, 1–3 sentences max, in a web-magazine tone.
- Avoid copying phrases verbatim.
- Do not add new facts.

Input:
Title: {title}
Summary: {summary}

Output strictly as JSON:
{{"header": "<rewritten title>", "summary": "<rewritten summary>"}}
"""

def rewrite_with_gpt(title: str, summary: str) -> dict:
    prompt = REWRITE_PROMPT_TEMPLATE.format(title=title.strip(), summary=summary.strip())

    try:
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.9,
            messages=[{"role": "user", "content": prompt}]
        )

        text = response.choices[0].message.content.strip()
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end != -1:
            try:
                json_data = json.loads(text[start:end+1])
                return {
                    "header": json_data.get("header", title).strip(),
                    "summary": json_data.get("summary", summary).strip()
                }
            except json.JSONDecodeError:
                pass

        lines = text.splitlines()
        header = lines[0].strip() if lines else title
        body = " ".join(lines[1:]).strip() if len(lines) > 1 else summary
        return {"header": header, "summary": body}

    except Exception as e:
        print(f"[GPT_REWRITE_ERROR] {e}")
        return {"header": title, "summary": summary}
