
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
from crawl4ai import ExtractionStrategy
from playwright.async_api import async_playwright
import traceback
from crawl4ai.extraction_strategy import RegexExtractionStrategy
# OpenAI API Key
client = OpenAI(api_key="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")


COMUNI_FILE = "C:\\Users\\39345\\Desktop\\TESI\\crawling\\prova_comuni.txt"
#COMUNI_FILE= "C:\\Users\\39345\\Desktop\\link_capoluoghi.txt"
df_comuni = pd.read_csv(COMUNI_FILE, header=None)
COMUNI = df_comuni[0].dropna().astype(str).tolist()



CSV_OUTPUT = os.path.expanduser("~/Desktop/tabella_finale.csv")


def normalizza_output(output):
    if isinstance(output, dict):
        return output
    elif hasattr(output, 'text'):
        return {"text": output.text, "content": output.content if hasattr(output, 'content') else ""}
    else:
        return {"text": str(output), "content": str(output)}




def carica_parole_chiave(path):
    with open(path, encoding="utf-8") as f:
        parole = [r.strip().lower() for r in f.readlines() if r.strip()]
    return set(parole)


#KEYWORDS_PATH= "C:\\Users\\39345\\Desktop\\TESI\\prova_parole.txt"
KEYWORDS_PATH = "C:\\Users\\39345\\Desktop\\TESI\\parole_solite.txt"
PAROLE_CHIAVE = carica_parole_chiave(KEYWORDS_PATH)


def contiene_procedimenti_per_keyword(html, parole_chiave, soglia=5):
    soup = BeautifulSoup(html, "html.parser")
    count = 0
    for link in soup.find_all("a", href=True):
        testo = link.get_text(strip=True).lower()
        href = link.get("href", "")
        if esclusi.search(href):
            continue
        for parola in parole_chiave:
            if parola in testo:
                count += 1
                break  
    return count >= soglia


def trova_link_regex(soup, pattern, url_base):
    for a in soup.find_all("a", href=True):
        testo = a.get_text(strip=True)
        if re.search(pattern, testo, flags=re.IGNORECASE):
            return urljoin(url_base, a["href"])
    return None




def estrai_dettagli_con_llm(testi, batch_size=5): 
    descrizioni = []
    normative = []
    riferimenti_temporali = []

    for i in range(0, len(testi), batch_size):
        batch = testi[i:i + batch_size]
        
        prompt = (
            "Sei un assistente esperto nella Pubblica Amministrazione italiana. "
            "Per ciascuno dei seguenti testi, estrai tre elementi fondamentali:\n"
            "- Descrizione del procedimento\n"
            "- Normativa di riferimento (es. articoli di legge, regolamenti)\n"
            "- Riferimenti temporali (es. scadenze, termini)\n\n"
            "Rispondi con una sola riga per testo, nel formato:\n"
            "<descrizione>|<normativa>|<riferimenti temporali>\n\n"
            "### Esempio:\n"
            "1) Il procedimento riguarda la richiesta per l’occupazione temporanea di suolo pubblico ai fini commerciali...\n"
            "→ Richiesta di occupazione suolo pubblico per attività temporanea|Art. 12 Regolamento comunale 45/2019|30 giorni dalla presentazione\n\n"
            "Adesso procedi con i seguenti testi:\n"
        )

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
        except Exception as e:
            print(f"Errore: {e}")
            for _ in batch:
                descrizioni.append("")
                normative.append("")
                riferimenti_temporali.append("")
    
    return descrizioni, normative, riferimenti_temporali


def estrai_descrizione_normativa(text):
    try:
        descrizioni, normative, riferimenti = estrai_dettagli_con_llm([text])
        return descrizioni[0], normative[0], riferimenti[0]
    except:
        print("  Errore LLM, uso regex fallback.")
        return estrai_fallback_regex(text)






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
    return desc, normativa, ""




esclusi = re.compile(
    r"(#|facebook|fb\.com|linkedin|lnkd\.in|twitter|x\.com|instagram|whatsapp|youtube|tiktok|mailto|cookie|ServeAttachment|ServeBLOB|\.pdf|\.docx?|\.xlsx?|\.pptx?)",
    re.IGNORECASE
)

async def estrai_info_da_comune(comune):
    base_url = comune.strip()
    risultati = []
    visitati = set()

    async with AsyncWebCrawler() as crawler:
        try:
            homepage = await crawler.arun(base_url, extraction_strategy=RegexExtractionStrategy(timeout=30000))
            homepage = normalizza_output(homepage)
            soup = BeautifulSoup(homepage["text"], "html.parser")

            link_trasp = trova_link_regex(soup, r"amministrazione\s+trasparente|trasparenza|trasparenti", base_url)
            if not link_trasp:
                link_amministrazione = trova_link_regex(soup, r"\\bamministrazione\\b", base_url)
                if link_amministrazione:
                    html_ammin = await crawler.arun(link_amministrazione, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                    html_ammin = normalizza_output(html_ammin)
                    soup_ammin = BeautifulSoup(html_ammin["text"], "html.parser")
                    link_trasp = trova_link_regex(soup_ammin, r"amministrazione\s+trasparente|trasparenza|trasparenti", link_amministrazione) or link_amministrazione
                else:
                    link_trasp = base_url

            html_trasp = await crawler.arun(link_trasp, extraction_strategy=RegexExtractionStrategy(timeout=30000))
            html_trasp = normalizza_output(html_trasp)
            soup_tr = BeautifulSoup(html_trasp["text"], "html.parser")

            candidati = []
            link_att = trova_link_regex(soup_tr, r"attivit[aà]\s+e\s+procediment[i]|procedimenti\s+e\s+attivit[aà]", link_trasp)
            if link_att:
                html_att = await crawler.arun(link_att, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                html_att = normalizza_output(html_att)
                soup_att = BeautifulSoup(html_att["text"], "html.parser")
                link_tip = trova_link_regex(soup_att, r"tipolog(ia|ie).*procediment[i]|procediment[i].*tipolog(ia|ie)|tipolog(ia|ie)", link_att)

                if link_tip:
                    html_tip = await crawler.arun(link_tip, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                    html_tip = normalizza_output(html_tip)
                    soup_tip = BeautifulSoup(html_tip["text"], "html.parser")

                    sotto_link = [
                        urljoin(link_tip, a["href"])
                        for a in soup_tip.find_all("a", href=True)
                        if (re.search(r"strutture\s+centrali|strutture\s+territoriali|database\s+dei\s+procedimenti|elenchi\s+dei\s+procedimenti", a.get_text(strip=True).lower())
                            and contiene_procedimenti_per_keyword(str(a), PAROLE_CHIAVE, soglia=1)
                            and not esclusi.search(a["href"]))
                    ]

                    trovato = False
                    for sl in sotto_link:
                        html_sub = await crawler.arun(sl, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                        html_sub = normalizza_output(html_sub)

                        if contiene_procedimenti_per_keyword(html_sub["text"], PAROLE_CHIAVE):
                            soup_sub = BeautifulSoup(html_sub["text"], "html.parser")
                            candidati = [
                                (a.get_text(strip=True), urljoin(sl, a["href"]))
                                for a in soup_sub.find_all("a", href=True)
                                if not esclusi.search(a["href"]) and any(p in a.get_text(strip=True).lower() for p in PAROLE_CHIAVE)
                            ]
                            trovato = True
                            break

                    if not trovato:
                        candidati = [
                            (a.get_text(strip=True), urljoin(link_tip, a["href"]))
                            for a in soup_tip.find_all("a", href=True)
                            if not esclusi.search(a["href"]) and any(p in a.get_text(strip=True).lower() for p in PAROLE_CHIAVE)
                        ]
                        

            if not candidati:
                link_doc = trova_link_regex(soup_tr, r"\\bdocumenti\\b.*\\bdati\\b|\\bdocumenti\\b|\\bdati\\b|\\batti\\b", link_trasp)
                if link_doc:
                    #print(" 'Documenti e dati' trovato.")
                    html_doc = await crawler.arun(link_doc, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                    html_doc = normalizza_output(html_doc)
                    if contiene_procedimenti_per_keyword(html_doc["text"], PAROLE_CHIAVE):
                        soup_doc = BeautifulSoup(html_doc["text"], "html.parser")
                        candidati = [
                            (a.get_text(strip=True), urljoin(link_doc, a["href"]))
                            for a in soup_doc.find_all("a", href=True)
                            if not esclusi.search(a["href"]) and any(p in a.get_text(strip=True).lower() for p in PAROLE_CHIAVE)
                        ]
                        

            for titolo, url_dettaglio in candidati:
                if url_dettaglio in visitati:
                    continue
                visitati.add(url_dettaglio)
                try:
                    dettaglio_html = await crawler.arun(url_dettaglio, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                    dettaglio_html = normalizza_output(dettaglio_html)
                    soup_det = BeautifulSoup(dettaglio_html["content"], "html.parser")
                    main = soup_det.find("main") or soup_det.find("div", id="main") or soup_det
                    testo = main.get_text(" ", strip=True)

                    descr, norma, ref = estrai_descrizione_normativa(testo[:2500])

                    risultati.append({
                        "Nome Attività": titolo,
                        "Descrizione": descr,
                        "Normativa": norma,
                        "Riferimenti temporali": ref,
                        "Output": "",
                        "Link": url_dettaglio,
                        "Comune": comune
                    })

                    await asyncio.sleep(1)

                except Exception as e:
                    print(f"Errore nel dettaglio {url_dettaglio}: {e}")

        except Exception as e:
            print(f" Errore generale per {comune}: {e}")
            traceback.print_exc()

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
