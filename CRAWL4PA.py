# NOTE:
# This is the original Crawl4PA script fully translated to English.
# Function names, variable names, and comments have been converted
# to English while preserving logic and structure.
# No refactoring or behavioral changes were intentionally introduced.

import os
import json
import time
import asyncio
import io
import re
import requests
import fitz  # PyMuPDF
import unicodedata
import pandas as pd
from bs4 import BeautifulSoup
from openai import OpenAI
from urllib.parse import urljoin, urlparse
from crawl4ai import AsyncWebCrawler
from playwright.async_api import async_playwright
import traceback
from crawl4ai.extraction_strategy import RegexExtractionStrategy
from pdf2image import convert_from_bytes
from PIL import Image
import pytesseract

# ------------------------------------------------------------
# OpenAI client (used for semantic extraction)
# ------------------------------------------------------------
client = OpenAI(api_key="YOUR_API_KEY_HERE")

# ------------------------------------------------------------
# INPUT: MUNICIPALITIES LIST
# ------------------------------------------------------------
MUNICIPALITIES_FILE = "C:\\Users\\39345\\Desktop\\TESI\\crawling\\prova_comuni.txt"

df_municipalities = pd.read_csv(MUNICIPALITIES_FILE, header=None)
MUNICIPALITIES = df_municipalities[0].dropna().astype(str).tolist()

# ------------------------------------------------------------
# OUTPUT CSV
# ------------------------------------------------------------
CSV_OUTPUT_PATH = os.path.expanduser("~/Desktop/tabella_prova.csv")

# ------------------------------------------------------------
# CRAWLER OUTPUT NORMALIZATION
# ------------------------------------------------------------
def normalize_output(output):
    """Normalize crawler output into a dict with a 'text' field."""
    if isinstance(output, dict):
        return output
    elif hasattr(output, 'text'):
        return {
            "text": output.text,
            "content": output.content if hasattr(output, 'content') else ""
        }
    else:
        return {"text": str(output), "content": str(output)}

# ------------------------------------------------------------
# KEYWORDS LOADING
# ------------------------------------------------------------
def load_keywords(path):
    """Load keywords from file and return them as a lowercase set."""
    with open(path, encoding="utf-8") as f:
        keywords = [r.strip().lower() for r in f.readlines() if r.strip()]
    return set(keywords)

KEYWORDS_PATH = "C:\\Users\\39345\\Desktop\\TESI\\parole_solite.txt"
KEYWORDS = load_keywords(KEYWORDS_PATH)

# ------------------------------------------------------------
# HEURISTIC: PAGE RELEVANCE CHECK
# ------------------------------------------------------------
def page_contains_procedures_by_keywords(html, keywords, threshold=5):
    """Check whether a page likely contains procedures based on keyword frequency."""
    soup = BeautifulSoup(html, "html.parser")
    count = 0
    for link in soup.find_all("a", href=True):
        text = link.get_text(strip=True).lower()
        href = link.get("href", "")
        if EXCLUDED_LINKS.search(href):
            continue
        for kw in keywords:
            if kw in text:
                count += 1
                break
    return count >= threshold

# ------------------------------------------------------------
# LINK DISCOVERY UTILITIES
# ------------------------------------------------------------
def find_link_by_regex(soup, pattern, base_url):
    """Return the first link whose anchor text matches the regex."""
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        if re.search(pattern, text, flags=re.IGNORECASE):
            return urljoin(base_url, a["href"])
    return None


def find_main_transparency_link(soup, pattern, base_url):
    """Find the main 'Amministrazione Trasparente' link."""
    candidates = []

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        href = a["href"]
        if re.search(pattern, text, flags=re.IGNORECASE):
            candidates.append((text.lower(), urljoin(base_url, href)))

    blacklist = ["rifiuti", "appalti", "tributi", "bilancio", "pago", "albo", "ecologia", "covid"]

    for text, url in candidates:
        if not any(bl in url.lower() or bl in text for bl in blacklist):
            return url

    return candidates[0][1] if candidates else None

# ------------------------------------------------------------
# LLM-BASED PROCEDURE EXTRACTION
# ------------------------------------------------------------
def extract_procedure_details_with_llm(texts, batch_size=5):
    """Use an LLM to extract procedure descriptions, laws, and time limits."""
    descriptions, laws, time_refs = [], [], []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]

        prompt = """
You are an assistant specialized in Italian Public Administration texts.
For each input text:
- Decide if it describes an administrative procedure.
- If yes, extract:
  1) Short description (max 15 words)
  2) Legal references
  3) Time constraints
- Otherwise output: NESSUN PROCEDIMENTO
Format: description|law|time
One line per text.
"""

        for idx, text in enumerate(batch, start=1):
            prompt += f"{idx}) {text.strip()}\n"

        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )

            lines = response.choices[0].message.content.strip().split("\n")
            for line in lines:
                parts = line.split("|")
                if len(parts) == 3:
                    d, l, t = parts
                    descriptions.append(d.strip())
                    laws.append(l.strip())
                    time_refs.append(t.strip())
                else:
                    descriptions.append("")
                    laws.append("")
                    time_refs.append("")
        except Exception:
            for _ in batch:
                descriptions.append("")
                laws.append("")
                time_refs.append("")

    return descriptions, laws, time_refs

# ------------------------------------------------------------
# FALLBACK: REGEX-BASED EXTRACTION
# ------------------------------------------------------------
def extract_fallback_with_regex(text):
    """Fallback extraction when LLM fails."""
    law = ""
    description = ""

    match = re.search(r"(art\.\s*\d+[^,.]{0,100})", text, re.IGNORECASE)
    if match:
        law = match.group(1)

    for paragraph in text.split("\n"):
        if len(paragraph.strip()) > 80 and "procedimento" in paragraph.lower():
            description = paragraph.strip()
            break

    return description, law, ""

# ------------------------------------------------------------
# EXCLUDED LINKS REGEX
# ------------------------------------------------------------
EXCLUDED_LINKS = re.compile(
    r"(#|facebook|fb\.com|linkedin|twitter|instagram|youtube|mailto|cookie|ServeAttachment|ServeBLOB)",
    re.IGNORECASE
)

# ------------------------------------------------------------
# MAIN ENTRY POINT
# ------------------------------------------------------------
async def main():
    """Main async entry point."""
    all_results = []

    for municipality in MUNICIPALITIES:
        results = await extract_information_from_municipality(municipality)
        all_results.extend(results)

    if all_results:
        df = pd.DataFrame(all_results)
        df.insert(1, "Activity ID", range(1, len(df) + 1))
        df = df[[
            "Activity Name",
            "Activity ID",
            "Description",
            "Legal Basis",
            "Time References",
            "Link",
            "Municipality"
        ]]
        df.to_csv(CSV_OUTPUT_PATH, index=False)
        print(f"CSV saved to: {CSV_OUTPUT_PATH}")
    else:
        print("No data collected.")


if __name__ == "__main__":
    asyncio.run(main())
