# ollama_paraphrase_helper.py
import requests, json, time
from difflib import SequenceMatcher

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "gemma2:9b"           # adjust to exact model name you have
TIMEOUT = 60

TITLE_SIM_THRESHOLD = 0.65
SUMMARY_SIM_THRESHOLD = 0.70
MAX_ATTEMPTS = 3

BASE_PROMPT_HEADER = """You are a professional editor. Rewrite the title and summary.
Rules:
- Preserve all factual details (names, dates, numbers, places).
- Do NOT invent new facts.
- PARAPHRASE AGGRESSIVELY: change phrasing, sentence structure, reorder clauses, and use synonyms.
- Aim to change at least 40% of words where possible while keeping meaning.
- Headline max 12 words. Summary 1-3 sentences.
Return only JSON: {"header":"...","summary":"..."}"""

FEW_SHOT = """
Example 1:
Input Title: Original: Artist X releases new album today
Input Summary: Original: Artist X dropped a new album today featuring pop and R&B tracks.
Output: {"header":"Artist X Drops New Pop-R&B Album","summary":"Artist X released a fresh album today blending pop and R&B styles."}

Example 2:
Input Title: Original: Famous actor shares behind-the-scenes photos
Input Summary: Original: The actor posted photos from the set showing candid moments.
Output: {"header":"Famous Actor Posts Candid Set Photos","summary":"The actor shared behind-the-scenes snaps from the set, giving fans a candid glimpse."}
"""

def similarity(a, b):
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.strip().lower(), b.strip().lower()).ratio()

def call_ollama(prompt, temp=0.9, top_p=0.95):
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "temperature": temp,
        "top_p": top_p,
        "max_tokens": 400,
        "stream": False
    }
    r = requests.post(OLLAMA_URL, json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    # try to extract text
    try:
        j = r.json()
        # common shapes: {"choices":[{"content":"..."}]}
        if isinstance(j, dict) and "choices" in j and j["choices"]:
            c = j["choices"][0]
            if isinstance(c, dict) and "content" in c:
                return c["content"] if isinstance(c["content"], str) else json.dumps(c["content"])
        # fallback to raw text
        return r.text
    except Exception:
        return r.text

def extract_json(text):
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start:end+1])
        except Exception:
            pass
    # fallback: naive split
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) >= 2:
        return {"header": lines[0], "summary": " ".join(lines[1:])}
    if lines:
        return {"header": lines[0], "summary": ""}
    return {"header": "", "summary": ""}

def paraphrase(title, summary):
    title = (title or "").strip()
    summary = (summary or "").strip()
    if not title and not summary:
        return {"header": title, "summary": summary}

    attempt = 0
    last = {"header": title, "summary": summary}
    while attempt < MAX_ATTEMPTS:
        attempt += 1
        extra = ""
        if attempt == 2:
            extra = "\nIf result is too similar, paraphrase more aggressively: reorder clauses and replace >40% words."
        if attempt >= 3:
            extra = "\nParaphrase extremely aggressively: use different sentence structure and synonyms. Preserve facts."

        prompt = f"{BASE_PROMPT_HEADER}\n{FEW_SHOT}\nInput Title: {title}\nInput Summary: {summary}\n{extra}\nOutput:"
        # adapt sampling: increase temp on later attempts
        temp = 0.9 + 0.05*(attempt-1)
        top_p = 0.95
        raw = call_ollama(prompt, temp=temp, top_p=top_p)
        parsed = extract_json(raw)
        header_out = parsed.get("header", title).strip()
        summary_out = parsed.get("summary", summary).strip()

        t_sim = similarity(title, header_out)
        s_sim = similarity(summary, summary_out)
        # accept only if both sufficiently different
        if t_sim <= TITLE_SIM_THRESHOLD and s_sim <= SUMMARY_SIM_THRESHOLD:
            return {"header": header_out, "summary": summary_out}
        last = {"header": header_out, "summary": summary_out}
        time.sleep(0.4 * attempt)

    # return last even if similar
    return last
