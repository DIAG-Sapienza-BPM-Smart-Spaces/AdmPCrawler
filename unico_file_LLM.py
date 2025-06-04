import os
import json
import time
import asyncio
import re
import pandas as pd
from bs4 import BeautifulSoup
from openai import OpenAI
from urllib.parse import urljoin
from crawl4ai import AsyncWebCrawler
from crawl4ai.extraction_strategy import BrowserHtmlExtractionStrategy

# OpenAI API Key
client = OpenAI(api_key="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")

COMUNI = ["roma", "milano", "napoli"]
KEYWORDS = ["adozione", "struttura", "richiesta"]
CSV_OUTPUT = os.path.expanduser("~/Desktop/tabella_finale_mirata.csv")

def conferma_se_link_e_procedimento(testo, url):
    prompt = f"""
Sei un assistente esperto della PA italiana.
Il seguente link ha come titolo:
"{testo}"
e come URL:
"{url}"
Questo link rimanda al dettaglio di un procedimento amministrativo (come adozione atti, accesso agli atti, richiesta autorizzazioni, ecc.)?
Rispondi solo con "sì" oppure "no".
"""
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return "sì" in response.choices[0].message.content.lower()
    except:
        return False

def estrai_con_batch_llm(testi, batch_size=5):
    descrizioni = []
    normative = []
    riferimenti_temporali = []

    for i in range(0, len(testi), batch_size):
        batch = testi[i:i + batch_size]
        prompt = "Per ciascuno dei seguenti testi, estrai: descrizione, riferimenti normativi (es. articoli di legge) e riferimenti temporali. Rispondi con una riga per testo, nel formato:\n<descrizione>|<normativa>|<riferimenti_temporali>\n\n"
        for idx, testo in enumerate(batch, start=1):
            prompt += f"{idx}) {testo.strip()}\n"

        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            righe = response.choices[0].message.content.strip().split("\n")
            for riga in righe:
                parts = riga.split("|")
                if len(parts) == 3:
                    desc, norma, ref = parts
                    descrizioni.append(desc.strip())
                    normative.append(norma.strip())
                    riferimenti_temporali.append(ref.strip())
                else:
                    descrizioni.append("")
                    normative.append("")
                    riferimenti_temporali.append("")
        except:
            for _ in batch:
                descrizioni.append("")
                normative.append("")
                riferimenti_temporali.append("")
    return descrizioni, normative, riferimenti_temporali

def estrai_descrizione_normativa(text):
    descrizioni, normative, riferimenti = estrai_con_batch_llm([text])
    return descrizioni[0], normative[0], riferimenti[0]

def estrai_fallback_regex(text):
    normativa = ""
    desc = ""
    norm_match = re.search(r"(art\.\s*\d+[^,.]{0,100})", text, re.IGNORECASE)
    if norm_match:
        normativa = norm_match.group(1)
    paragraphs = text.split("\n")
    for p in paragraphs:
        if len(p.strip()) > 80 and "procedimento" in p.lower():
            desc = p.strip()
            break
    return desc, normativa

async def seleziona_link_post_tipologie(candidati, crawler, comune):
    esclusi = re.compile(r"(#|facebook|linkedin|twitter|cookie|whatsapp|tiktok|social|servizi)", re.IGNORECASE)
    pattern_validi = re.compile(r"(strutture centrali|database dei procedimenti|elenchi dei procedimenti)", re.IGNORECASE)
    for text, link in candidati:
        if len(text.strip()) > 4 and not esclusi.search(link) and pattern_validi.search(text):
            try:
                html = await crawler.arun(link, extraction_strategy=BrowserHtmlExtractionStrategy(timeout=30000))
                if "procedimento" in html.text.lower():
                    print(f"\n Verificato e selezionato: '{text}' → {link}")
                    return link
            except:
                continue
    print(f"\n Nessun link valido trovato dopo verifica contenuto per {comune}")
    return None

async def estrai_info_da_comune(comune):
    base_url = f"https://www.comune.{comune}.it"
    print(f"\n Analisi in corso: {comune.upper()} - {base_url}")
    risultati = []
    async with AsyncWebCrawler() as crawler:
        try:
            homepage = await crawler.arun(base_url, extraction_strategy=BrowserHtmlExtractionStrategy(timeout=30000))
            soup = BeautifulSoup(homepage.text, "html.parser")

            def trova_link(soup, testo):
                for a in soup.find_all("a", href=True):
                    if testo.lower() in a.get_text(strip=True).lower():
                        return urljoin(base_url, a["href"])
                return None

            link_trasp = trova_link(soup, "trasparente") or trova_link(soup, "amministrazione")
            if not link_trasp:
                return []
            html1 = await crawler.arun(link_trasp, extraction_strategy=BrowserHtmlExtractionStrategy(timeout=30000))
            soup1 = BeautifulSoup(html1.text, "html.parser")
            link_att = trova_link(soup1, "attività e procedimenti")
            if not link_att:
                return []
            html2 = await crawler.arun(link_att, extraction_strategy=BrowserHtmlExtractionStrategy(timeout=30000))
            soup2 = BeautifulSoup(html2.text, "html.parser")
            link_tip = trova_link(soup2, "tipologie di procedimento")
            if not link_tip:
                return []
            html3 = await crawler.arun(link_tip, extraction_strategy=BrowserHtmlExtractionStrategy(timeout=30000))
            soup3 = BeautifulSoup(html3.text, "html.parser")
            candidati = [(a.get_text(strip=True), urljoin(link_tip, a["href"])) for a in soup3.find_all("a", href=True)]
            link_elenco = await seleziona_link_post_tipologie(candidati, crawler, comune)
            if not link_elenco:
                return []

            html4 = await crawler.arun(link_elenco, extraction_strategy=BrowserHtmlExtractionStrategy(timeout=30000))
            soup4 = BeautifulSoup(html4.text, "html.parser")
            for a in soup4.find_all("a", href=True):
                titolo = a.get_text(strip=True)
                if any(k in titolo.lower() for k in KEYWORDS):
                    url_dettaglio = urljoin(link_elenco, a["href"])
                    if not conferma_se_link_e_procedimento(titolo, url_dettaglio):
                        continue
                    dettaglio_html = await crawler.arun(url_dettaglio, extraction_strategy=BrowserHtmlExtractionStrategy(timeout=30000))
                    testo = BeautifulSoup(dettaglio_html.text, "html.parser").get_text()
                    descrizione, normativa, ref_temp = estrai_descrizione_normativa(testo)
                    if not descrizione and not normativa:
                        descrizione, normativa = estrai_fallback_regex(testo)
                    risultati.append({
                        "Nome Attività": titolo,
                        "Descrizione": descrizione,
                        "Normativa": normativa,
                        "Riferimenti temporali": ref_temp,
                        "Link": url_dettaglio,
                        "Comune": comune
                    })
                    time.sleep(1)
        except Exception as e:
            print(f"Errore generale per {comune}: {e}")
    return risultati

async def main():
    tutti = []
    for comune in COMUNI:
        risultati = await estrai_info_da_comune(comune)
        tutti.extend(risultati)

    if tutti:
        df = pd.DataFrame(tutti)
        df.insert(1, "ID Attività", range(1, len(df) + 1))
        df = df[["Nome Attività", "ID Attività", "Descrizione", "Normativa", "Riferimenti temporali", "Link", "Comune"]]
        df.to_csv(CSV_OUTPUT, index=False)
        print(f" File salvato: {CSV_OUTPUT}")
    else:
        print(" Nessun dato raccolto.")

if __name__ == "__main__":
    asyncio.run(main())
