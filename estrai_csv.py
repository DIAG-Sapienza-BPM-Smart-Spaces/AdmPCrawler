import pandas as pd
import os
import re
import asyncio
from urllib.parse import urljoin
from playwright.async_api import async_playwright
from openai import OpenAI


CSV_INPUT = "C:\\Users\\39345\\Desktop\\estrai_csv\\prove_link.csv"
OUTPUT_DIR = "norme_csv_procedimenti"
os.makedirs(OUTPUT_DIR, exist_ok=True)

client = OpenAI(api_key="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")

pattern_azione = re.compile(
    r"(presentare|richiedere|allegare|inoltrare|effettuare|"
    r"compilare|consegnare|trasmettere|accompagnata|inviare|"
    r"dimostrare|sottoscrivere|iscriversi|effettuata|ottenere|"
    r"produrre|autenticare|depositare|fornire|ritirare|firmare|"
    r"partecipare|essere tenuto a|\u00e8 necessario|\u00e8 richiesto|"
    r"viene.*presentata|deve.*essere|si.*provvede.*a)",
    re.IGNORECASE
)

pattern_da_pulire = re.compile(
    r"(art\\.?\\s*\\d+[^\\w]*)|"
    r"(articolo\\s*\\d+)|"
    r"(legge\\s*n\\.?\\s*\\d+/?\\d*)|"
    r"(d\\.?lgs\\.?\\s*\\d+/?\\d*)|"
    r"(^\\s*\\d+[\\.)]\\s*)|"
    r"(^\\s*[a-z]\)\s*)",
    re.IGNORECASE
)

async def estrai_testo_dal_dom(page):
    try:
        contenuto = await page.locator("main, #main-content, article, .content, body").inner_text()
        return contenuto.strip()
    except:
        return ""

def estrai_frasi_procedurali_con_llm(testo):
    if len(testo.strip()) < 200:
        return []

    frasi = re.split(r'(?<=[\.!?])\s+', testo)
    frasi_candidati = []
    for frase in frasi:
        if pattern_azione.search(frase) and len(frase.split()) >= 5:
            frase_pulita = pattern_da_pulire.sub("", frase).strip()
            frasi_candidati.append(frase_pulita)

    if not frasi_candidati:
        return []

    joined = "\n".join(f"- {f}" for f in frasi_candidati[:20])
    prompt = f"""
Le seguenti frasi sono tratte da articoli di legge o regolamento. Seleziona solo quelle che descrivono azioni o adempimenti da parte di cittadini o imprese nell'ambito di un procedimento amministrativo (es. fare una domanda, ottenere un'autorizzazione, allegare documenti, partecipare a un bando). Indica anche se è presente un termine temporale (es. \"entro 30 giorni\", \"non oltre il termine di scadenza\"). Non duplicare le frasi, riporta ogni frase una sola volta.

Esempi validi:
- Presentare istanza di autorizzazione entro 30 giorni
- Allegare i documenti richiesti
- Compilare il modulo disponibile
- Richiedere la certificazione sanitaria
- Partecipare al bando entro i termini stabiliti
- Consegnare copia conforme del contratto
- Effettuare il pagamento entro 10 giorni dalla notifica
- Depositare la domanda presso l’ufficio competente

Frasi:
{joined}

Rispondi con un elenco puntato delle sole frasi procedurali, identiche a quelle fornite, senza modificarle e senza aggiungere spiegazioni.
"""

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    output = response.choices[0].message.content
    frasi_valide = list(set([line.strip("-• ").strip() for line in output.splitlines() if line.strip()]))
    return frasi_valide

async def analizza_norma(page, url, index):
    print(f"Analizzo: {url}")
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(2000)

    articoli_links = []

    try:
        await page.wait_for_selector("a")
        anchors = await page.locator("a").all()
        for a in anchors:
            try:
                text = (await a.inner_text()).strip().lower()
                href = await a.get_attribute("href")
                if (
                    href and
                    not href.startswith("javascript:") and
                    not href.endswith(".pdf") and
                    ("art" in text or re.search(r"\b(articolo|capo|sezione|\d{1,3})\b", text))
                ):
                    full_url = urljoin(url, href)
                    if full_url not in articoli_links:
                        articoli_links.append(full_url)
                        #print(f" Link articolo trovato: {text} -> {full_url}")
            except:
                continue
    except Exception as e:
        print(f"Errore durante raccolta articoli: {e}")

    if not articoli_links:
        print(" Nessun link articolo trovato.")
        return

    #print(f"Articoli trovati: {len(articoli_links)}")
    frasi_finali = []

    for i, link in enumerate(articoli_links):
        try:
            print(f"[{i+1}/{len(articoli_links)}] Apro articolo: {link}")
            await page.goto(link, timeout=60000)
            await page.wait_for_timeout(1000)
            testo = await estrai_testo_dal_dom(page)
            if len(testo.strip()) < 100:
                continue
            frasi = estrai_frasi_procedurali_con_llm(testo)
            frasi_finali.extend(frasi)
        except Exception as e:
            print(f"Errore articolo: {e}")

    frasi_finali = list(set(frasi_finali))
    for frase in frasi_finali:
        print(" -", frase)

    nome_file = f"norma_{index+1}.csv"
    path_csv = os.path.join(OUTPUT_DIR, nome_file)
    df_out = pd.DataFrame({
        "Frasi procedurali": frasi_finali or ["(nessuna frase rilevata)"]
    })
    df_out.to_csv(path_csv, index=False)
    print(f"Salvato: {nome_file} ({len(frasi_finali)} frasi)")

async def main():
    df = pd.read_csv(CSV_INPUT)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        for index, row in df.iterrows():
            url = row.get("Link normativa", "")
            if isinstance(url, str) and url.startswith("http"):
                await analizza_norma(page, url, index)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

