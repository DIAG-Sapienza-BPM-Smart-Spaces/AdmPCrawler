
import os
import json
import time
import asyncio
import io
import re
import requests
import fitz  
import pandas as pd
from bs4 import BeautifulSoup
from openai import OpenAI
from urllib.parse import urljoin
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler
from playwright.async_api import async_playwright
import traceback
from crawl4ai.extraction_strategy import RegexExtractionStrategy
from pdf2image import convert_from_bytes
from PIL import Image
import pytesseract


# OpenAI API Key
client = OpenAI(api_key="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")


COMUNI_FILE= "C:\\Users\\39345\\Desktop\\link_capoluoghi.txt"
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
        prompt=( """
Contesto = 
Sei un assistente intelligente specializzato nell’analisi e classificazione di testi normativi e informativi presenti sui siti web della Pubblica Amministrazione italiana. Il tuo compito è quello di identificare, se presenti, i procedimenti amministrativi descritti nei testi e di estrarre informazioni strutturate da essi.

Input = 
Una serie di blocchi testuali (uno o più paragrafi ciascuno) estratti da siti web di enti pubblici italiani. Ogni blocco può contenere descrizioni di attività, procedure, riferimenti normativi, informazioni temporali o solo contenuti generici.

Obiettivo = 
Per ciascun blocco di testo ricevuto, segui i seguenti passaggi:
1. Determina se il testo descrive effettivamente un **procedimento amministrativo** (ossia un’attività che comporta un iter, una richiesta da parte dell’utente e/o una decisione dell’amministrazione).
2. Se sì, estrai tre elementi distinti:
   - Una **descrizione sintetica** (massimo 15 parole) che riassuma il procedimento.
   - Gli **eventuali riferimenti normativi** (articoli di legge, regolamenti, delibere).
   - Gli **eventuali riferimenti temporali** (termini, scadenze, durate, giorni entro cui fare la richiesta, ecc.).
3. Se invece il testo **non** descrive un procedimento, rispondi con una sola parola chiave: `NESSUN PROCEDIMENTO`.

Vincoli = 
- Se un campo non è presente nel testo, lascia vuoto ma mantieni la struttura.
- La risposta deve essere **una riga per ciascun testo**.
- Gli elementi devono essere separati da un pipe `|`.
- Non aggiungere commenti, introduzioni, né numerazioni.

Output = 
Una lista di righe nel formato:
<descrizione procedimento>|<normativa di riferimento>|<riferimenti temporali>

Esempi = 

Input:
Il procedimento riguarda la richiesta per l’occupazione temporanea di suolo pubblico ai fini commerciali. La domanda deve essere presentata almeno 30 giorni prima dell’evento. Fa riferimento all’art. 12 del Regolamento comunale 45/2019.

Output:
Richiesta occupazione suolo pubblico|Art. 12 Regolamento comunale 45/2019|30 giorni prima dell’evento

Input:
La pagina fornisce informazioni generiche sul portale salute e sicurezza e non rappresenta un iter amministrativo formale.

Output:
NESSUN PROCEDIMENTO

---

Ora applica le istruzioni per i testi che seguono.
"""
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




def estrai_link_da_pdf(url_pdf):
    urls = []
    try:
        r = requests.get(url_pdf, timeout=30)
        r.raise_for_status()
        with io.BytesIO(r.content) as f:
            doc = fitz.open("pdf", f.read())
            for page in doc:
                links = page.get_links()
                for l in links:
                    if "uri" in l:
                        urls.append(l["uri"])
    except Exception as e:
        print(f"Errore estraendo link da PDF {url_pdf}: {e}")
    return urls



async def estrai_pdf_playwright(url):
    """
    Estrae i link ai PDF da una pagina usando Playwright.
    Restituisce una lista di URL completi dei PDF trovati.
    """
    pdf_links = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("networkidle")

            anchors = await page.query_selector_all("a[href]")

            for a in anchors:
                href = await a.get_attribute("href")
                if href and ".pdf" in href.lower():
                    href = urljoin(url, href)
                    pdf_links.append(href)

            await browser.close()

    except Exception as e:
        print(f"Errore Playwright: {e}")

    return pdf_links




def estrai_testo_pdf_pymupdf(pdf_url):
    """
    Scarica il PDF da un URL e ritorna una lista di stringhe
    """
    testo_righe = []
    try:
        
        parsed = urlparse(pdf_url)
        referer = f"{parsed.scheme}://{parsed.netloc}"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": referer
        }

        r = requests.get(pdf_url, headers=headers, timeout=30)
        r.raise_for_status()

        with io.BytesIO(r.content) as f:
            doc = fitz.open("pdf", f.read())
            testo_completo = ""
            for page in doc:
                testo_completo += page.get_text() + "\n"

            testo_righe = [r.strip() for r in testo_completo.split("\n") if r.strip()]

    except Exception as e:
        print(f"Errore estraendo testo da PDF {pdf_url}: {e}")

    return testo_righe



def scarica_pdf_requests(url, comune):
    """
    Scarica il PDF nella cartella procedimenti_pdf/NOME_COMUNE
    """
    parsed = urlparse(url)
    referer = f"{parsed.scheme}://{parsed.netloc}"

    headers_base = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        ),
        "Referer": referer
    }

    response = requests.get(url, headers=headers_base)
    if response.status_code != 200:
        print(f"Errore download PDF: {response.status_code}")
        return None

    
    nome_file = os.path.basename(url.split("?")[0])

    if "://" in comune:
        
        parsed_comune = urlparse(comune)
        comune_norm = parsed_comune.netloc.replace(".", "_")
    else:
        comune_norm = comune.replace(" ", "_")

    base_dir = os.path.expanduser("~/Desktop/procedimenti_pdf")
    cartella_comune = os.path.join(base_dir, comune_norm)
    os.makedirs(cartella_comune, exist_ok=True)

    percorso_pdf = os.path.join(cartella_comune, nome_file)

    with open(percorso_pdf, "wb") as f:
        f.write(response.content)

    print(f" PDF scaricato in: {percorso_pdf}")
    return percorso_pdf



def estrai_blocchi_puliti_da_pdf(percorso_pdf):
    """
    Estrae blocchi di testo puliti da un PDF.
    """
    blocchi = []
    try:
        doc = fitz.open(percorso_pdf)
        testo_completo = ""
        for pagina in doc:
            testo_completo += pagina.get_text("text") + "\n"

        raw_blocchi = re.split(r"\n\s*\n|\n(?=PROCEDIMENTO|\bProcedimento\b)", testo_completo)
        for b in raw_blocchi:
            pulito = []
            for riga in b.splitlines():
                r = riga.strip()
                if not r:
                    continue
                if any(k in r.lower() for k in ["tel", "pec", "mail", "@", "fax"]):
                    continue
                pulito.append(r)
            blocco = " ".join(pulito).strip()
            if blocco:
                blocchi.append(blocco)
    except Exception as e:
        print(f"Errore estraendo testo da {percorso_pdf}: {e}")
    return blocchi


def procedimenti_da_blocchi(blocchi, link_pdf, comune):
    """
    Converte una lista di blocchi di testo in una lista di dizionari di procedimenti.
    """
    procedimenti = []
    for blocco in blocchi:
        # Per Nome Attività prendi la prima riga significativa o una riga contenente "PROCEDIMENTO"
        nome_attivita = ""
        righe = blocco.splitlines()
        for r in righe:
            if "procedimento" in r.lower():
                nome_attivita = r.strip()
                break
        if not nome_attivita:
            nome_attivita = righe[0].strip() if righe else "Procedimento"

        # Normativa: prima riga con "art."
        normativa = next((r for r in righe if "art." in r.lower()), "")
        # Riferimento temporale: prima riga con "giorni"
        riferimento = next((r for r in righe if "giorni" in r.lower()), "")

        procedimenti.append({
            "Nome Attività": nome_attivita,
            "Descrizione": blocco,
            "Normativa": normativa,
            "Riferimenti temporali": riferimento,
            "Output": "",
            "Link": link_pdf,
            "Comune": comune
        })

    return procedimenti




def estrai_procedimenti_con_fallback(url_pdf, comune):
    """
    Scarica un PDF da URL e prova prima l'estrazione testo,
    poi l'OCR se necessario.
    Restituisce una lista di procedimenti.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    response = requests.get(url_pdf, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Errore download PDF (status {response.status_code})")

    pdf_bytes = response.content


    try:
        testo_pymupdf = ""
        doc = fitz.open(stream=io.BytesIO(pdf_bytes), filetype="pdf")
        for pagina in doc:
            testo_pymupdf += pagina.get_text("text") + "\n"
        testo_pymupdf = testo_pymupdf.strip()
    except Exception as e:
        print(f"Errore PyMuPDF: {e}")
        testo_pymupdf = ""

    procedimenti_pymupdf = _estrai_blocchi_da_testo(testo_pymupdf, url_pdf, comune)

    if procedimenti_pymupdf:
        print(f"Estratti {len(procedimenti_pymupdf)} procedimenti con PyMuPDF.")
        return procedimenti_pymupdf

    print(" Nessun procedimento trovato con PyMuPDF, passo a OCR...")

    
    immagini = convert_from_bytes(pdf_bytes)
    testo_ocr = ""
    for img in immagini:
        testo_ocr += pytesseract.image_to_string(img, lang="ita") + "\n"
    testo_ocr = testo_ocr.strip()

    procedimenti_ocr = _estrai_blocchi_da_testo(testo_ocr, url_pdf, comune)

    print(f" Estratti {len(procedimenti_ocr)} procedimenti con OCR.")
    return procedimenti_ocr

def _estrai_blocchi_da_testo(testo, url_pdf, comune):
    """
    Segmenta in blocchi ogni volta che trova una riga che inizia con 'PROCEDIMENTO',
    e passa ogni blocco all'LLM per estrarre descrizione, normativa e riferimenti.
    """
    righe = testo.splitlines()
    procedimenti = []
    blocco_corrente = []
    nome_attivita_corrente = ""

    for riga in righe:
        riga_pulita = riga.strip()
        if not riga_pulita:
            continue

        # Nuovo procedimento se la riga inizia con PROCEDIMENTO
        if riga_pulita.upper().startswith("PROCEDIMENTO"):
            if blocco_corrente:
                testo_blocco = " ".join(blocco_corrente)
                descr, norma, ref = estrai_descrizione_normativa(testo_blocco[:2500])

                procedimenti.append({
                    "Nome Attività": nome_attivita_corrente,
                    "Descrizione": descr,
                    "Normativa": norma,
                    "Riferimenti temporali": ref,
                    "Output": "",
                    "Link": url_pdf,
                    "Comune": comune
                })

            nome_attivita_corrente = riga_pulita
            blocco_corrente = []
        else:
            blocco_corrente.append(riga_pulita)

    # Salva l'ultimo blocco
    if nome_attivita_corrente and blocco_corrente:
        testo_blocco = " ".join(blocco_corrente)
        descr, norma, ref = estrai_descrizione_normativa(testo_blocco[:2500])

        procedimenti.append({
            "Nome Attività": nome_attivita_corrente,
            "Descrizione": descr,
            "Normativa": norma,
            "Riferimenti temporali": ref,
            "Output": "",
            "Link": url_pdf,
            "Comune": comune
        })

    print(f"Suddiviso in {len(procedimenti)} procedimenti.")
    return procedimenti


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
                link_amministrazione = trova_link_regex(soup, r"\bamministrazione\b", base_url)
                if link_amministrazione:
                    html_ammin = await crawler.arun(link_amministrazione, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                    html_ammin = normalizza_output(html_ammin)
                    soup_ammin = BeautifulSoup(html_ammin["text"], "html.parser")
                    link_trasp = trova_link_regex(
                        soup_ammin,
                        r"amministrazione\s+trasparente|trasparenza|trasparenti",
                        link_amministrazione
                    ) or link_amministrazione
                else:
                    link_trasp = base_url

            html_trasp = await crawler.arun(link_trasp, extraction_strategy=RegexExtractionStrategy(timeout=30000))
            html_trasp = normalizza_output(html_trasp)
            soup_tr = BeautifulSoup(html_trasp["text"], "html.parser")

            candidati = []

            link_att = trova_link_regex(
                soup_tr,
                r"attivit[aà]\s+e\s+procediment[i]|procedimenti\s+e\s+attivit[aà]",
                link_trasp
            )

            fallback_attivato = False

            if link_att:
                html_att = await crawler.arun(link_att, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                html_att = normalizza_output(html_att)
                soup_att = BeautifulSoup(html_att["text"], "html.parser")

                link_tip = trova_link_regex(
                    soup_att,
                    r"tipolog(ia|ie).*procediment[i]|procediment[i].*tipolog(ia|ie)|tipolog(ia|ie)",
                    link_att
                )
                

                if link_tip:
                        try:
                            html_tip = await crawler.arun(link_tip, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                            html_tip = normalizza_output(html_tip)
                            soup_tip = BeautifulSoup(html_tip["text"], "html.parser")

                            # 1 Cerca PDF nella pagina tipologie
                            pdf_links = [
                                urljoin(link_tip, a["href"])
                                for a in soup_tip.find_all("a", href=True)
                                if a["href"].lower().endswith(".pdf") and not esclusi.search(a["href"])
                            ]
                            print(f"Trovati {len(pdf_links)} link PDF nella pagina tipologie.")
                            if not pdf_links:
                                print("Provo Playwright per i PDF dinamici...")
                                pdf_links = await estrai_pdf_playwright(link_tip)

                            for pdf_url in pdf_links:
                                candidati.append(("PDF Procedimento", pdf_url))

                            # 2 Cerca sotto-link procedimenti amministrativi
                            sotto_link = [
                                urljoin(link_tip, a["href"])
                                for a in soup_tip.find_all("a", href=True)
                                if (
                                        re.search(r"procedimenti\s+amministrativi", a.get_text(strip=True).lower())
                                        and not esclusi.search(a["href"])
                                    )
                            ]

                            print(f"Trovati {len(sotto_link)} sotto-link strutture/centrali/territoriali.")

                            # Variabile per controllare se ho trovato parole chiave nei sotto-link
                            trovato_kw_sotto_link = False

                            # 3 Per ciascun sotto-link: cerca PDF e parole chiave
                            for sl in sotto_link:
                                try:
                                    print(f"Scarico sotto-link: {sl}")
                                    html_sub = await crawler.arun(sl, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                                    html_sub = normalizza_output(html_sub)
                                    soup_sub = BeautifulSoup(html_sub["text"], "html.parser")

                                    # 3.a Cerca PDF nel sotto-link
                                    pdf_sub_links = [
                                        urljoin(sl, a["href"])
                                        for a in soup_sub.find_all("a", href=True)
                                        if a["href"].lower().endswith(".pdf") and not esclusi.search(a["href"])
                                    ]
                                    print(f"Trovati {len(pdf_sub_links)} PDF nel sotto-link {sl}.")
                                    for pdf_url in pdf_sub_links:
                                        candidati.append(("PDF Procedimento", pdf_url))

                                    # 3.b Cerca link con parole chiave nel sotto-link
                                    candidati_sub_kw = [
                                        (a.get_text(strip=True), urljoin(sl, a["href"]))
                                        for a in soup_sub.find_all("a", href=True)
                                        if not esclusi.search(a["href"]) and any(
                                            p in a.get_text(strip=True).lower() for p in PAROLE_CHIAVE
                                        )
                                    ]
                                    print(f"Trovati {len(candidati_sub_kw)} link con parole chiave nel sotto-link {sl}.")
                                    if candidati_sub_kw:
                                        trovato_kw_sotto_link = True
                                    candidati.extend(candidati_sub_kw)

                                except Exception as e:
                                    print(f"Errore caricando il sotto-link {sl}: {e}. Ignoro e continuo.")

                            # 4 Solo se non ho trovato parole chiave nei sotto-link, cerca nella pagina tipologie
                            if not trovato_kw_sotto_link:
                                candidati_kw = [
                                    (a.get_text(strip=True), urljoin(link_tip, a["href"]))
                                    for a in soup_tip.find_all("a", href=True)
                                    if not esclusi.search(a["href"]) and any(
                                        p in a.get_text(strip=True).lower() for p in PAROLE_CHIAVE
                                    )
                                ]
                                print(f"Trovati {len(candidati_kw)} link con parole chiave nella pagina tipologie.")
                                candidati.extend(candidati_kw)
                            else:
                                print("Ho trovato parole chiave nei sotto-link, quindi salto la ricerca nella pagina principale.")




                        except Exception as e:
                            print(f"Errore caricando Tipologie di Procedimento: {e}")
                            fallback_attivato = True

            if fallback_attivato:
                print("Esecuzione fallback: cerco in Documenti/Dati e Database Procedimenti")
                link_doc = trova_link_regex(
                    soup_tr,
                    r"\bdocumenti\b.*\bdati\b|\bdocumenti\b|\bdati\b|\batti\b|database\s+procediment[i]",
                    link_trasp
                )
                if link_doc:
                    html_doc = await crawler.arun(link_doc, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                    html_doc = normalizza_output(html_doc)
                    if contiene_procedimenti_per_keyword(html_doc["text"], PAROLE_CHIAVE):
                        soup_doc = BeautifulSoup(html_doc["text"], "html.parser")
                        candidati = [
                            (a.get_text(strip=True), urljoin(link_doc, a["href"]))
                            for a in soup_doc.find_all("a", href=True)
                            if not esclusi.search(a["href"]) and any(
                                p in a.get_text(strip=True).lower() for p in PAROLE_CHIAVE
                            )
                        ]

            for titolo, url_dettaglio in candidati:
                if url_dettaglio in visitati:
                    continue
                visitati.add(url_dettaglio)

                try:
                    if url_dettaglio.lower().endswith(".pdf"):
                                print("Scarico PDF:", url_dettaglio)
                                percorso_pdf = scarica_pdf_requests(url_dettaglio, comune)

                                if percorso_pdf:
                                    procedimenti =estrai_procedimenti_con_fallback(url_dettaglio, comune)

                                    print(f"Trovati {len(procedimenti)} procedimenti nel PDF.")
                                    risultati.extend(procedimenti)
                                else:
                                    print("PDF non scaricato correttamente, nessun procedimento aggiunto.")



                    else:
                        
                        dettaglio_html = await crawler.arun(
                            url_dettaglio,
                            extraction_strategy=RegexExtractionStrategy(timeout=30000)
                        )
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
            print(f"Errore generale per {comune}: {e}")
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
