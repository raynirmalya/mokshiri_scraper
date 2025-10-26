# gpt_rewriter_expanded.py
from openai import OpenAI
import json
import os
from dotenv import load_dotenv

# Load your API key
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("❌ OPENAI_API_KEY is missing. Add it to .env")

client = OpenAI(api_key=api_key)

EXPANDED_PROMPT_TEMPLATE = """
You are an expert entertainment and lifestyle editor.

Your job is to rewrite and enrich the given article title and summary so it feels natural, engaging, and detailed for web readers.

### Rules:
- Keep all facts accurate (names, dates, numbers, places).
- Make the headline catchy, SEO-friendly (≤ 12 words).
- Expand the summary into a richer, 2–5 sentence mini-article (~80–150 words).
- Add human warmth, context, and storytelling flow — like a lifestyle journalist.
- Avoid repetitive phrasing or AI tone.
- Do NOT invent new events or quotes.
- Use smooth transitions and descriptive language.

### Example
Input:
Title: BLACKPINK member releases new solo song
Summary: The BLACKPINK star shared her solo track online today.

Output JSON:
{{"header":"BLACKPINK’s Star Drops Her First Solo Song","summary":"BLACKPINK’s member unveiled her first solo release today, blending pop and R&B rhythms. Fans worldwide celebrated the drop across social platforms, calling it one of the most anticipated debuts of the year."}}

### Now rewrite this:
Title: {title}
Summary: {summary}

Output strictly as JSON:
{{"header":"<rewritten title>","summary":"<expanded human-like summary>"}}
"""

def rewrite_with_gpt_expanded(title: str, summary: str) -> dict:
    """Generate a richer, more human-like rewrite using GPT-4.1-mini."""
    prompt = EXPANDED_PROMPT_TEMPLATE.format(title=title.strip(), summary=summary.strip())

    try:
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.9,
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.choices[0].message.content.strip()
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end != -1:
            try:
                data = json.loads(text[start:end+1])
                return {
                    "header": data.get("header", title).strip(),
                    "summary": data.get("summary", summary).strip(),
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
