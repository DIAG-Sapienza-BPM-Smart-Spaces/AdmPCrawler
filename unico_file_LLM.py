
import os
import json
import time
import asyncio
import io
import re
import requests
import fitz  
import unicodedata
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


client = OpenAI(api_key="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")


COMUNI_FILE = "C:\\Users\\39345\\Desktop\\TESI\\crawling\\prova_comuni.txt"
#COMUNI_FILE= "C:\\Users\\39345\\Desktop\\link_capoluoghi.txt"
df_comuni = pd.read_csv(COMUNI_FILE, header=None)
COMUNI = df_comuni[0].dropna().astype(str).tolist()



CSV_OUTPUT = os.path.expanduser("~/Desktop/tabella_prova.csv")


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

def trova_link_trasparenza_principale(soup, pattern, url_base):
    candidati = []

    for a in soup.find_all("a", href=True):
        testo = a.get_text(strip=True)
        href = a["href"]
        if re.search(pattern, testo, flags=re.IGNORECASE):
            full_url = urljoin(url_base, href)
            candidati.append((testo.lower(), full_url))

    
    blacklist = ["rifiuti", "appalti", "tributi", "bilancio", "pago", "albo", "ecologia", "covid"]

    for testo, url in candidati:
        if not any(bl in url.lower() or bl in testo for bl in blacklist):
            return url  

    if candidati:
        return candidati[0][1] 

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
        print(" Errore LLM, uso regex.")
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
    r"(#|facebook|fb\.com|linkedin|lnkd\.in|twitter|x\.com|instagram|whatsapp|youtube|tiktok|mailto|cookie|ServeAttachment|ServeBLOB)",
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

    procedimenti = []
    for blocco in blocchi:
        nome_attivita = ""
        righe = blocco.splitlines()
        for r in righe:
            if "procedimento" in r.lower():
                nome_attivita = r.strip()
                break
        if not nome_attivita:
            nome_attivita = righe[0].strip() if righe else "Procedimento"

        
        normativa = next((r for r in righe if "art." in r.lower()), "")
        
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
    procedimenti = []
    parole_chiave = PAROLE_CHIAVE  
    

    
    raw_blocchi = re.split(r"\n\s*\n", testo)

    for raw_blocco in raw_blocchi:
        blocco = " ".join([r.strip() for r in raw_blocco.splitlines() if r.strip()])
        if not blocco:
            continue

        # Verifica se il blocco contiene almeno una parola chiave
        blocco_lower = blocco.lower()
        if any(kw in blocco_lower for kw in parole_chiave):
            descr, norma, ref = estrai_descrizione_normativa(blocco[:2500])


            if descr.strip().upper() != "NESSUN PROCEDIMENTO":
                procedimenti.append({
                    "Nome Attività": descr if descr else "Procedimento",
                    "Descrizione": descr,
                    "Normativa": norma,
                    "Riferimenti temporali": ref,
                    "Output": "",
                    "Link": url_pdf,
                    "Comune": comune
                })

    print(f" Suddiviso e filtrato in {len(procedimenti)} procedimenti tramite parole chiave.")
    return procedimenti

async def fallback_playwright_trova_link_multipli(url, patterns):

    print(f" Fallback Playwright: cerco link corrispondente a uno tra: {patterns}")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("domcontentloaded")

            anchors = await page.query_selector_all("a[href]")
            for a in anchors:
                try:
                    testo = (await a.inner_text()).strip().lower()
                    href = await a.get_attribute("href")
                    if not href:
                        continue

                    for pattern in patterns:
                        if re.search(pattern, testo, flags=re.IGNORECASE):
                            full_url = urljoin(url, href)
                            print(f" Trovato con Playwright: {full_url}")
                            await browser.close()
                            return full_url

                except:
                    continue

            await browser.close()
            print(" Nessun link trovato con Playwright.")
    except Exception as e:
        print(f" Errore Playwright fallback: {e}")

    return None




def _pagination_candidates(soup, base_url):
    
    seen = set()
    out = []

    keys_href = ["page=", "pagina=", "/page/", "/pagina/", "offset=", "start=", "PageNo=", "p="]
    pat_arrow = re.compile(r"^\s*(»|>|›|»»|>>)\s*$")
    pat_next = re.compile(r"(successiv|prossim|avanti|next|pagina|pag\.)", re.IGNORECASE)

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        txt = a.get_text(" ", strip=True)
        rel = a.get("rel")

        is_rel_next = False
        if rel:
            if isinstance(rel, list):
                is_rel_next = any(str(r).lower() == "next" for r in rel)
            else:
                is_rel_next = "next" in str(rel).lower()

        cond = (
            is_rel_next
            or any(k in href.lower() for k in keys_href)
            or txt.isdigit()
            or pat_arrow.search(txt)
            or pat_next.search(txt)
        )

        if cond:
            full = urljoin(base_url, href)
            if full not in seen:
                seen.add(full)
                out.append(full)
    return out


async def iter_pagination(start_url, crawler, max_pages=50):
    
    root = urlparse(start_url).netloc
    q = deque([start_url])
    visited = set()

    while q and len(visited) < max_pages:
        url = q.popleft()
        if url in visited:
            continue
        visited.add(url)

        html = await crawler.arun(url, extraction_strategy=RegexExtractionStrategy(timeout=30000))
        html = normalizza_output(html)
        soup = BeautifulSoup(html["text"], "html.parser")

        #print(f"[PAG] Analizzo: {url}")
        yield url, soup

        # raccogli i candidati alla "pagina successiva"
        for cand in _pagination_candidates(soup, url):
            if urlparse(cand).netloc == root and cand not in visited and cand not in q:
                q.append(cand)

# Pattern testo per riconoscere "sottosezioni" organizzative dove spesso sono i procedimenti
PATTERN_SOTTOSEZIONI = re.compile(
    r"\b("
    r"catalogo|elenco\s+procediment[io]i|"
    r"elenco\s+procediment[ia]\s+amministrativ[ia]"
    r"settore(?:\s+[a-zà-ù]+){0,6}|"
    r"dipartiment[oi](?:\s+[a-zà-ù]+){0,6}|"
    r"area(?:\s+[a-zà-ù]+){0,6}|"
    r"struttur[ea](?:\s+[a-zà-ù]+){0,6}|"
    r"unit[aà]\s+organizzativa(?:\s+[a-zà-ù]+){0,6}|"
    r"serviz[io]i?(?:\s+[a-zà-ù]+){0,6}|"
    r"uffic[io]i?(?:\s+[a-zà-ù]+){0,6}"
    r")\b",
    re.IGNORECASE
)


PAROLE_STRONG = [
    "procedimenti", "procedimento", "tipologie", "tipologia",
    "attività e procedimenti", "attività", "schede", "scheda",
    "modulistica", "moduli", "istanza", "domanda",
    "uffici", "strutture", "servizi", "settori", "area", "aree",
    "normativa", "regolamenti", "atti normativi",
    # extra comuni
    "procedimenti e servizi", "carta dei servizi", "trasparenza",
    "amministrazione trasparente", "sportello", "urp", "servizi online"
]


FILE_EXTS = (".pdf", ".csv", ".xlsx", ".xls")

def _dbg(msg): 
    print(f"[SOTTOSEZIONI][DBG] {msg}")

def _strip_accents_lower(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower().strip()

def _same_section_relaxed(base, target):
    """Consenti sottodomini dello stesso eTLD+1 (es. comune.tld e sub.comune.tld)."""
    try:
        bp, tp = urlparse(base), urlparse(target)
        if not bp.hostname or not tp.hostname:
            return False
        base_host = ".".join(bp.hostname.split(".")[-2:])
        targ_host = ".".join(tp.hostname.split(".")[-2:])
        return base_host == targ_host
    except Exception:
        return False

def _contains_keywords(text, parole_chiave, soglia=2):
    t = _strip_accents_lower(text)
    count = 0
    for kw in parole_chiave:
        k = _strip_accents_lower(kw)
        if not k:
            continue
        # parola intera quando possibile
        rx = re.compile(rf"\b{re.escape(k)}\b")
        count += len(rx.findall(t))
    return count >= soglia

def _link_text_candidates(a):
    """Ritorna testo combinato: innerText + title + aria-label, normalizzato."""
    txt  = a.get_text(" ", strip=True) if a else ""
    tit  = a.get("title", "")
    aria = a.get("aria-label", "")
    return _strip_accents_lower(" ".join([txt or "", tit or "", aria or ""]))



async def crawl_sottosezioni_bfs(start_soup, start_url, crawler, parole_chiave, esclusi, max_depth=3, soglia_kw=2):
    from collections import deque
    
    def _same_section(a, b): 
        return _same_section_relaxed(a, b)

    risultati = []
    visitati = set()
    q = deque()
    q.append((start_url, start_soup, 0))
    visitati.add(start_url)

    while q:
        page_url, soup, depth = q.popleft()
        if depth > max_depth:
            continue

        
        all_links = list(soup.find_all("a", href=True))
        _dbg(f"URL={page_url} depth={depth} link_totali={len(all_links)}")

        
        file_links = [
            urljoin(page_url, a["href"])
            for a in all_links
            if not esclusi.search(a["href"])
               and a["href"].lower().endswith(FILE_EXTS)
        ]
        if file_links:
            _dbg(f"FILE trovati: {len(file_links)}")
            for u in file_links:
                label = "PDF Procedimento" if u.lower().endswith(".pdf") else "File Procedimenti"
                risultati.append((label, u))

        
        kw_links = []
        for a in all_links:
            href = a["href"]
            if esclusi.search(href):
                continue
            testo_combo = _link_text_candidates(a)
            if not testo_combo:
                continue

            
            strong_hit = any(s in testo_combo for s in map(_strip_accents_lower, PAROLE_STRONG))
            kw_hit     = any(_strip_accents_lower(kw) in testo_combo for kw in parole_chiave)

            
            href_low = _strip_accents_lower(href)
            name_hit = any(_strip_accents_lower(kw) in href_low for kw in parole_chiave)

            if (strong_hit and kw_hit) or name_hit:
                full = urljoin(page_url, href)
                if _same_section(start_url, full):
                    titolo_vis = (a.get_text(" ", strip=True) or a.get("title") or a.get("aria-label") or "Link procedimento").strip()
                    kw_links.append((titolo_vis, full))

        if kw_links:
            _dbg(f"KW link selezionati: {len(kw_links)}")
            risultati.extend(kw_links)

        
        nuovi = []
        for a in all_links:
            if esclusi.search(a["href"]):
                continue
            testo = a.get_text(" ", strip=True) or ""
            if not testo:
                continue
            if PATTERN_SOTTOSEZIONI.search(testo):
                full = urljoin(page_url, a["href"])
                if _same_section(start_url, full):
                    nuovi.append(full)

        _dbg(f"sottosezioni_cand={len(nuovi)}")

        
        for url_next in nuovi:
            if url_next in visitati:
                continue
            visitati.add(url_next)
            try:
                html_n = await crawler.arun(url_next, extraction_strategy=RegexExtractionStrategy(timeout=35000))
                html_n = normalizza_output(html_n)
                testo_page = html_n.get("text") or ""
                if not _contains_keywords(testo_page, parole_chiave, soglia=soglia_kw):
                    _dbg(f"[SKIP] {url_next}: segnali insufficienti (kw < {soglia_kw}).")
                    
                    if not _contains_keywords(testo_page, parole_chiave, soglia=1):
                        continue
                soup_n = BeautifulSoup(testo_page, "html.parser")
                q.append((url_next, soup_n, depth + 1))
                _dbg(f"expand -> {url_next} (depth {depth+1})")
            except Exception as e:
                print(f"[SOTTOSEZIONI] errore su {url_next}: {e}")

    
    dedup = {}
    for titolo, urlx in risultati:
        dedup[urlx] = (titolo, urlx)

    out = list(dedup.values())
    _dbg(f"RISULTATI FINALI: {len(out)}")
    return out





def trova_inner_trasparenze_ordinate(soup, base_url):
    """
    Raccoglie TUTTI i link 'Amministrazione trasparente' presenti nella pagina
    (bottoni, link testuali, ecc.), li normalizza su base_url e li ordina
    dal più recente al meno recente in base all'anno (heuristic).
    """
    candidati = []
    RX_TRASP = re.compile(
        r"amministrazione\s+trasparent[ea].*|^\s*trasparenza\s*$|^\s*amministrazione\s*$",
        re.IGNORECASE
    )

    for a in soup.find_all("a", href=True):
        testo = a.get_text(" ", strip=True)
        if RX_TRASP.search(testo):
            candidati.append((testo, urljoin(base_url, a["href"])))

    
    visti = set()
    unici = []
    for t, u in candidati:
        if u not in visti:
            visti.add(u)
            unici.append((t, u))

    #
    def score(item):
        t, u = item
        anni = re.findall(r"(20\d{2})", t + " " + u)
        if anni:
            try:
                return max(int(x) for x in anni)
            except:
                return 0
        
        if re.search(r"\bdal\b", t, re.IGNORECASE): return 9999
        if re.search(r"\bfino\s+al\b", t, re.IGNORECASE): return 1
        return 0

    unici.sort(key=score, reverse=True)
    return [u for _, u in unici]



def _trova_tipologie_inner_links(soup, base_url):
    RX_TIP = re.compile(
        r"tipolog(?:ia|ie)\s*(?:dei|de|di)?\s*procediment[i]|"
        r"\bcatalogo\s+(?:dei\s+)?procediment[i]\b|"
        r"\belenco\s+(?:dei\s+)?procediment[i]\b|"
        r"\bschede?\s+procediment[i]\b|"
        r"\bprocedimenti\s+amministrativi\b|"
        r"\btipolog(?:ia|ie)\b|"
        r"\bprocedur[ae]\b",
        re.IGNORECASE
    )
    urls = []
    for a in soup.find_all("a", href=True):
        t = a.get_text(" ", strip=True)
        if RX_TIP.search(t):
            urls.append(urljoin(base_url, a["href"]))

    
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)

    
    def _depth(u):
        p = urlparse(u).path.rstrip("/")
        return (p.count("/"), len(p))
    out.sort(key=_depth, reverse=True)
    return out
async def estrai_info_da_comune(comune):
    base_url = comune.strip()
    risultati = []
    visitati = set()

    
    async def doppio_hop_attivita_procedimenti(link_trasp, soup_tr, crawler):
   
           
            rx_att = r"attivit[aà]\s*e\s*procediment[i]|procediment[i]\s*e\s*attivit[aà]"
            link_att = trova_link_regex(soup_tr, rx_att, link_trasp)
            if link_att:
                return link_att

            
            inner_list = trova_inner_trasparenze_ordinate(soup_tr, link_trasp)

            
            inner_list = [u for u in inner_list if u != link_trasp]

            for inner in inner_list:
                try:
                    html_tr2 = await crawler.arun(inner, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                    html_tr2 = normalizza_output(html_tr2)
                    soup_tr2 = BeautifulSoup(html_tr2["text"], "html.parser")

                    
                    link_att_inner = trova_link_regex(soup_tr2, rx_att, inner)
                    if link_att_inner:
                        return link_att_inner

                except Exception as e:
                    print(f"[ATT] Errore aprendo inner trasparenza {inner}: {e}")

            
            patterns_pw = [
                r"\battivit[aà]\b",
                r"\bprocediment[i]\b",
                r"attivit[aà].*procediment[i]|procediment[i].*attivit[aà]",
                r"\btipolog(ia|ie)\b",
                r"\bnormativa\b|\batti\s+normativi\b",
            ]
            try:
                link_att_pw = await fallback_playwright_trova_link_multipli(link_trasp, patterns_pw)
                if link_att_pw:
                    return link_att_pw
            except Exception as e:
                print(f"[ATT] Playwright corrente fallito: {e}")

            
            for inner in inner_list:
                try:
                    link_att_pw_inner = await fallback_playwright_trova_link_multipli(inner, patterns_pw)
                    if link_att_pw_inner:
                        return link_att_pw_inner
                except Exception as e:
                    print(f"[ATT] Playwright inner {inner} fallito: {e}")

            
            return None

    async def doppio_hop_tipologie(link_att, soup_att, crawler):
        
        link_tip = trova_link_regex(
            soup_att,
            r"tipolog(?:ia|ie)\s*(?:dei|de|di)?\s*procediment[i]|catalogo\s+(?:dei\s+)?procediment[i]|"
            r"elenco\s+(?:dei\s+)?procediment[i]|schede?\s+procediment[i]|procedimenti\s+amministrativi|"
            r"\btipolog(?:ia|ie)\b|\bprocedur[ae]\b",
            link_att
        )
        if not link_tip:
            patterns = [
                r"\btipolog(?:ia|ie)\s*(?:dei|de|di)?\s*procediment[i]\b",
                r"\bcatalogo\s+(?:dei\s+)?procediment[i]\b",
                r"\belenco\s+(?:dei\s+)?procediment[i]\b",
                r"\bschede?\s+procediment[i]\b",
                r"\bprocedimenti\s+amministrativi\b",
                r"\bprocedur[ae]\b",
                r"\btipolog(?:ia|ie)\b",
            ]
            link_tip = await fallback_playwright_trova_link_multipli(link_att, patterns)

        
        if link_tip:
            try:
                pdf_links_global = []

                
                async for _, soup_tip_first in iter_pagination(link_tip, crawler, max_pages=1):
                    inner_tip_list = _trova_tipologie_inner_links(soup_tip_first, link_tip)
                
                inner_tip_list = [u for u in inner_tip_list if u != link_tip]

                
                tipologie_da_visitare = inner_tip_list + [link_tip]

                visitati_tip = set()
                for tip_url in tipologie_da_visitare:
                    if tip_url in visitati_tip:
                        continue
                    visitati_tip.add(tip_url)

                    async for page_url, soup_tip in iter_pagination(tip_url, crawler, max_pages=50):
                        
                        pdf_links = [
                            urljoin(page_url, a["href"])
                            for a in soup_tip.find_all("a", href=True)
                            if a["href"].lower().endswith(".pdf")
                        ]
                        print(f"[Tipologie] {page_url} -> PDF trovati: {len(pdf_links)}")
                        pdf_links_global.extend(pdf_links)

                        
                        sotto_link = await crawl_sottosezioni_bfs(
                            start_soup=soup_tip,
                            start_url=tip_url,
                            crawler=crawler,
                            parole_chiave=PAROLE_CHIAVE,
                            esclusi=esclusi,
                            max_depth=3
                        )
                        print(f"[Tipologie] sottosezioni: raccolti {len(sotto_link)} elementi (PDF o link con parole chiave).")
                        candidati.extend(sotto_link)

                        
                        candidati_kw_page = [
                            (a.get_text(strip=True), urljoin(page_url, a["href"]))
                            for a in soup_tip.find_all("a", href=True)
                            if not esclusi.search(a["href"]) and any(
                                p in a.get_text(strip=True).lower() for p in PAROLE_CHIAVE
                            )
                        ]
                        print(f"[Tipologie] {page_url} -> KW: {len(candidati_kw_page)}")
                        candidati.extend(candidati_kw_page)

                
                base_per_dyn = inner_tip_list[0] if inner_tip_list else link_tip
                if not pdf_links_global:
                    print("Provo Playwright per i PDF dinamici (Tipologie, pagina profonda se presente)…")
                    pdf_links_global = await estrai_pdf_playwright(base_per_dyn)

                for pdf_url in pdf_links_global:
                    candidati.append(("PDF Procedimento", pdf_url))

            except Exception as e:
                print(f"Errore caricando Tipologie di Procedimento (multi-hop): {e}")
        fallback_attivato = True

        
        return None
   

    async with AsyncWebCrawler() as crawler:
        try:
            homepage = await crawler.arun(base_url, extraction_strategy=RegexExtractionStrategy(timeout=30000))
            homepage = normalizza_output(homepage)
            soup = BeautifulSoup(homepage["text"], "html.parser")

            
            link_trasp = trova_link_trasparenza_principale(
                soup,
                r"amministrazione\s+trasparente|trasparenza|trasparenti",
                base_url
            )
            if not link_trasp:
                link_amministrazione = trova_link_regex(soup, r"\bamministrazione\b", base_url)
                if link_amministrazione:
                    html_ammin = await crawler.arun(link_amministrazione, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                    html_ammin = normalizza_output(html_ammin)
                    soup_ammin = BeautifulSoup(html_ammin["text"], "html.parser")
                    link_trasp = (
                        trova_link_regex(soup_ammin, r"amministrazione\s+trasparente|trasparenza|trasparenti", link_amministrazione)
                        or link_amministrazione
                    )
                else:
                    link_trasp = base_url 

            
            html_trasp = await crawler.arun(link_trasp, extraction_strategy=RegexExtractionStrategy(timeout=30000))
            html_trasp = normalizza_output(html_trasp)
            soup_tr = BeautifulSoup(html_trasp["text"], "html.parser")

            candidati = []
            fallback_attivato = False

            
            link_att = await doppio_hop_attivita_procedimenti(link_trasp, soup_tr, crawler)

            if link_att:
                html_att = await crawler.arun(link_att, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                html_att = normalizza_output(html_att)
                soup_att = BeautifulSoup(html_att["text"], "html.parser")

                
                link_tip = await doppio_hop_tipologie(link_att, soup_att, crawler)

                if link_tip:
                    try:
                        pdf_links_global = []

                        
                        async for page_url, soup_tip in iter_pagination(link_tip, crawler, max_pages=50):
                            
                            pdf_links = [
                                urljoin(page_url, a["href"])
                                for a in soup_tip.find_all("a", href=True)
                                if a["href"].lower().endswith(".pdf")
                            ]
                            print(f"[Tipologie] {page_url} -> PDF trovati: {len(pdf_links)}")
                            pdf_links_global.extend(pdf_links)

                            
                            sotto_link = await crawl_sottosezioni_bfs(
                                start_soup=soup_tip,
                                start_url=link_tip,
                                crawler=crawler,
                                parole_chiave=PAROLE_CHIAVE,
                                esclusi=esclusi,
                                max_depth=3
                            )
                            print(f"[Tipologie] sottosezioni: raccolti {len(sotto_link)} elementi (PDF o link con parole chiave).")
                            candidati.extend(sotto_link)

                            
                            candidati_kw_page = [
                                (a.get_text(strip=True), urljoin(page_url, a["href"]))
                                for a in soup_tip.find_all("a", href=True)
                                if not esclusi.search(a["href"]) and any(
                                    p in a.get_text(strip=True).lower() for p in PAROLE_CHIAVE
                                )
                            ]
                            print(f"[Tipologie] {page_url} -> KW: {len(candidati_kw_page)}")
                            candidati.extend(candidati_kw_page)

                        
                        if not pdf_links_global:
                            print("Provo Playwright per i PDF dinamici (Tipologie)...")
                            pdf_links_global = await estrai_pdf_playwright(link_tip)

                        for pdf_url in pdf_links_global:
                            candidati.append(("PDF Procedimento", pdf_url))

                    except Exception as e:
                        print(f"Errore caricando Tipologie di Procedimento: {e}")
                        fallback_attivato = True

                else:
                    
                    print("[Info] Nessuna sezione 'Tipologie di procedimento'. Cerco comunque PDF/keyword in 'Attività e procedimenti'.")
                    
                    pdf_att = [
                        urljoin(link_att, a["href"])
                        for a in soup_att.find_all("a", href=True)
                        if a["href"].lower().endswith(".pdf")
                    ]
                    for u in pdf_att:
                        candidati.append(("PDF Procedimento", u))

                    
                    candidati_kw_att = [
                        (a.get_text(strip=True), urljoin(link_att, a["href"]))
                        for a in soup_att.find_all("a", href=True)
                        if not esclusi.search(a["href"]) and any(
                            p in a.get_text(strip=True).lower() for p in PAROLE_CHIAVE
                        )
                    ]
                    candidati.extend(candidati_kw_att)

                    
                    if not pdf_att:
                        print("Provo Playwright per i PDF dinamici (Attività e procedimenti)...")
                        pdf_dyn = await estrai_pdf_playwright(link_att)
                        for u in pdf_dyn:
                            candidati.append(("PDF Procedimento", u))
            else:
                print ("Non ho trovato 'Attività e procedimenti': passo a Fallback esteso")
                fallback_attivato = True


            
            if fallback_attivato and not candidati:
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

                
                if not candidati:
                    print("Fallback esteso: cerco sezioni con atti normativi, norme o procedure...")
                    soup_finale = BeautifulSoup(html_trasp["text"], "html.parser")
                    pattern_norme = re.compile(r"\b(norme|normativa|atti\s+normativi|procedure|procedura)\b", re.IGNORECASE)

                    link_norme = None
                    for a in soup_finale.find_all("a", href=True):
                        testo = a.get_text(strip=True)
                        if pattern_norme.search(testo):
                            link_norme = urljoin(link_trasp, a["href"])
                            print(f"Trovato link a sezione normativa: {link_norme}")
                            break

                    if link_norme:
                        try:
                            html_norme = await crawler.arun(link_norme, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                            html_norme = normalizza_output(html_norme)
                            soup_norme = BeautifulSoup(html_norme["text"], "html.parser")

                            candidati_norme = [
                                (a.get_text(strip=True), urljoin(link_norme, a["href"]))
                                for a in soup_norme.find_all("a", href=True)
                                if not esclusi.search(a["href"]) and any(
                                    p in a.get_text(strip=True).lower() for p in PAROLE_CHIAVE
                                )
                            ]
                            print(f"Trovati {len(candidati_norme)} link con parole chiave nella sezione normativa.")
                            candidati.extend(candidati_norme)
                        except Exception as e:
                            print(f"Errore accedendo alla sezione normativa: {e}")

            
            for titolo, url_dettaglio in candidati:
                if url_dettaglio in visitati:
                    continue
                visitati.add(url_dettaglio)

                try:
                    if isinstance(titolo, str) and titolo.lower().startswith("pdf"):
                        
                        pass

                    if url_dettaglio.lower().endswith(".pdf"):
                        print("Scarico PDF:", url_dettaglio)
                        percorso_pdf = scarica_pdf_requests(url_dettaglio, comune)
                        if percorso_pdf:
                            procedimenti = estrai_procedimenti_con_fallback(url_dettaglio, comune)
                            print(f"Trovati {len(procedimenti)} procedimenti nel PDF.")
                            risultati.extend(procedimenti)
                        else:
                            print("PDF non scaricato correttamente, nessun procedimento aggiunto.")
                    else:
                        
                        dettaglio_html = await crawler.arun(url_dettaglio, extraction_strategy=RegexExtractionStrategy(timeout=30000))
                        dettaglio_html = normalizza_output(dettaglio_html)
                        soup_det = BeautifulSoup(dettaglio_html["text"], "html.parser")
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

                        await asyncio.sleep(0.5)

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
