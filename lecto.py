import os
from flask import json
import requests

apiKey = os.getenv('LECTO_API_KEY')

headers = {
    'X-API-Key': "BC1K70A-KTXMK6W-QTT3FMF-Z4MKC52",
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}

payload = {
    "texts": ["MAMA 2025 Unveils Star-Studded Lineup From BABYMONSTER to SUPER JUNIOR", "The excitement is building as the 2025 Mnet Asian Music Awards (MAMA) announces its first lineup of 17 sensational performers, blending fresh faces and legendary icons in one spectacular event. Kicking off with acts like rookie groups BABYMONSTER and ZEROBASEONE alongside veterans such as SUPER JUNIOR and Stray Kids, this diverse roster promises a vivid showcase of K-pop’s dynamic evolution. Set over two electrifying nights, each with its own theme, MAMA 2025 embraces the spirit of “Uh Heung” (어흥) — a Korean expression of joy and vibrant energy that unites fans through music and dance. Fans can look forward to innovative stage productions, thrilling collaborations, and cutting-edge technology that highlight K-pop’s global influence. With more performers yet to be revealed, this year’s MAMA is poised to captivate audiences worldwide once again, celebrating over two decades of Asia’s premier music festival."],
    "to": ["ko"],
    "from": "en"
}

#data = '{\n        "texts": ["The excitement is building as the 2025 Mnet Asian Music Awards (MAMA) announces its first lineup of 17 sensational performers, blending fresh faces and legendary icons in one spectacular event. Kicking off with acts like rookie groups BABYMONSTER and ZEROBASEONE alongside veterans such as SUPER JUNIOR and Stray Kids, this diverse roster promises a vivid showcase of K-pop’s dynamic evolution. Set over two electrifying nights, each with its own theme, MAMA 2025 embraces the spirit of “Uh Heung” (어흥) — a Korean expression of joy and vibrant energy that unites fans through music and dance. Fans can look forward to innovative stage productions, thrilling collaborations, and cutting-edge technology that highlight K-pop’s global influence. With more performers yet to be revealed, this year’s MAMA is poised to captivate audiences worldwide once again, celebrating over two decades of Asia’s premier music festival."],\n        "to": ["ko", "ja"],\n        "from": "en"\n    }'

response = requests.post('https://api.lecto.ai/v1/translate/text', headers=headers, data=json.dumps(payload))

print(response.headers)
print(response.json())