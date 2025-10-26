import requests
import json

# ---------- CONFIG ----------
COOKIE_STRING = ("awsCntryCode=10; acesCntry=00; dlvCntry=10; currency=USD; curLang=en; lang=en; guest_id=644a7aea-e4f5-4884-b638-6b1432eda32c; _gcl_au=1.1.275898204.1761217640; _ga=GA1.1.1713184827.1761217640; _yjsu_yjad=1761217641.186a5bdc-2ca2-41b0-bab0-341af9a896d3; _scid=MgP-eR0dhTEUE5JYUZsUUWwYuvs6G0Jp; _ScCbts=%5B%5D; _pin_unauth=dWlkPVpESTJPR05oTVRrdE5EWmxZUzAwTXpoaExXSTNOekl0T1dSa1ltWXlOMlF3TVdWag; _sctr=1%7C1761157800000; lantern=a1247206-ec77-4b8b-9008-37240cc19b5a; dcard-adkt-device=75f3001d-adc2-468f-8a93-7e6f42d4a13c; _scid_r=S4P-eR0dhTEUE5JYUZsUUWwYuvs6G0JpHIMhkA; _rdt_uuid=1761217643496.4c4a1b4a-bb79-436b-9d16-a32a9eb6ac9f; cto_bundle=9F1tgF9sbndYMTJWZFdlMiUyQkJkSDJxRVpBaFNPbG0zbW1ieVZpNnNGWEExdTMzNTJockhDUzBwSCUyRmROaTh2bWpHNUl4QWdIRXpPQWtUNjFsOEVvZEt6cHBlVThleEdyeGFNU0V6QWs1V3AxJTJCSUlqMGd5U2FtdFc3eWdxdXJLTk4wZTNsb1BTZDJJd00wU05wZEVtSXZqOXp4cXVpeTNPS09zcDJzQk9xaVNUTCUyRjdyYyUzRA; _ga_5ZDXC4W9LE=GS2.1.s1761217639$o1$g1$t1761217651$j48$l0$h0; ab.storage.deviceId.6ec5aad2-ce7e-415f-808c-d6ae870076fd=g%3A0e6dcdf9-072a-9658-e042-dfd2ca2a0e5a%7Ce%3Aundefined%7Cc%3A1761217629706%7Cl%3A1761359669555; ab.storage.userId.6ec5aad2-ce7e-415f-808c-d6ae870076fd=g%3AvdR2g4mMPt0G%252B7dBUfeKvQ%253D%253D%7Ce%3Aundefined%7Cc%3A1761359669555%7Cl%3A1761359669556; ck_ag_pop=N; pageMoveCnt2025%2F10%2F25=0; FOSID=NTA1MThhNjAtNjI4MS00OWRlLWExODMtN2YzZDgyNmY5YmU5; AMP_ab4732f3fb=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjIwZTZkY2RmOS0wNzJhLTk2NTgtZTA0Mi1kZmQyY2EyYTBlNWElMjIlMkMlMjJ1c2VySWQlMjIlM0ElMjJ2ZFIyZzRtTVB0MEclMkI3ZEJVZmVLdlElM0QlM0QlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzYxMzU5NTI2NTQzJTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc2MTM2MDcxNzkzMiUyQyUyMmxhc3RFdmVudElkJTIyJTNBMTEzJTJDJTIycGFnZUNvdW50ZXIlMjIlM0EyMiU3RA==; ab.storage.sessionId.6ec5aad2-ce7e-415f-808c-d6ae870076fd=g%3A5dcd51e7-ddc8-c988-2e0a-675cb83d4a97%7Ce%3A1761362518600%7Cc%3A1761359669555%7Cl%3A1761360718600; _dd_s=rum=0&expire=1761361684520")  # paste full cookie
X_CSRF_TOKEN = "209a20c6-2338-4167-8b0a-ff1b4e7fc4fd"  # paste the x-csrf-token value
PRDTNO = "GA210002919"         # product id
RWARD = "MOKSHIRI18"           # your reward code (rwardCode)
SHOTURL_ENDPOINT = "https://global.oliveyoung.com/influencer/shotUrl"

# ---------- HEADERS (match browser request) ----------
headers = {
    "authority": "global.oliveyoung.com",
    "method": "POST",
    "scheme": "https",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json; charset=UTF-8",
    "origin": "https://global.oliveyoung.com",
    "referer": "https://global.oliveyoung.com/influencer/program/creating-content",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "x-csrf-token": X_CSRF_TOKEN,
    "x-requested-with": "XMLHttpRequest",
    # Cookie must be one string exactly as in devtools
    "cookie": COOKIE_STRING,
}

payload = {"originUrl": f"/product/detail?prdtNo={PRDTNO}&rwardCode={RWARD}&utm_source=influencers"}

resp = requests.post(SHOTURL_ENDPOINT, json=payload, headers=headers, timeout=15)
print("Status:", resp.status_code)
try:
    data = resp.json()
except json.JSONDecodeError:
    print("Response text:", resp.text)
    raise

print("Response JSON:", json.dumps(data, indent=2))
# likely field: data['shortUrl'] or similar
print("shortUrl:", data.get("shortUrl") or data.get("short_url") or data)
