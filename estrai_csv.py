 #questo funziona lòaprima parte
import asyncio
import pandas as pd
import json
import os
import re
from playwright.async_api import async_playwright
from openai import OpenAI

# === CONFIG ===


CSV_INPUT = "C:\\Users\\39345\\Desktop\\norme_link.csv"
#CSV_INPUT = "C:\\Users\\39345\\Desktop\\tabella_finale_firenze_with_links.csv"
CSV_OUTPUT_FOLDER = "C:\\Users\\39345\\Desktop\\estrai_csv\\output_norme\\"
os.makedirs(CSV_OUTPUT_FOLDER, exist_ok=True)


client = OpenAI(api_key="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")




def genera_prompt_etichettatura(indice_text):
    return f"""
Contesto:
Hai ricevuto l'indice di un atto normativo. Ogni riga rappresenta un titolo o articolo, con il relativo numero e descrizione. Il tuo compito è identificare quali voci, in base al titolo, potrebbero descrivere un procedimento amministrativo.

Obiettivo:
Restituire una lista di articoli che secondo te contengono o introducono un procedimento, escludendo quelli introduttivi o definitori.

Input:
{indice_text}

Output:
- Una lista contenente i numeri e i titoli degli articoli che, secondo la tua analisi, includono la descrizione di una ***procedura amministrativa*** (cioè una sequenza operativa, attività regolata, fasi autorizzative, passaggi tecnici o provvedimenti amministrativi).
- L’output deve indicare accanto a ogni titolo: → [procedura] oppure → [no]
- Se alcuni articoli non sembrano contenere una procedura, etichettali come [no].

Parole chiave indicative di procedura:
- procedimento, procedura, istanza, domanda, presentazione
- rilascio, concessione, autorizzazione, permesso
- controllo, verifica, ispezione, accertamento
- affidamento, aggiudicazione, indizione, gara
- comunicazione, notifica, pubblicazione
- catalogazione, conservazione, approvazione
- conferenza di servizi, ricorso, contributo, valutazione
- espropriazione, premiazione, dichiarazione, trasferimento

Parole da escludere (non procedurali):
- definizione, oggetto, ambito, finalità, principio
- disposizioni generali, norme transitorie, abrogazione, beni culturali
- funzioni, valori, patrimonio, ambiti di applicazione

Approccio step-by-step:
1. Leggi l’indice riga per riga.
2. Per ogni riga, valuta solo il titolo dell’articolo.
3. Se il titolo suggerisce uno dei temi elencati nelle parole chiave procedurali, etichettalo come → [procedura].
4. Se il titolo è introduttivo, definitorio o descrive solo principi astratti, etichettalo come → [no].
5. Restituisci la lista nel formato indicato: numero, titolo, etichetta.

Vincoli:
- Rispondi solo con testo etichettato, una riga per articolo.
- Non aggiungere commenti, spiegazioni o testo aggiuntivo.
- L’output deve essere leggibile e parsabile da un sistema automatico.

Audience:
Questo output verrà usato da un sistema di crawling automatico che seleziona solo gli articoli ritenuti “procedurali” per essere analizzati in dettaglio da un secondo modello.
"""




def debug_pulisci_testo_normativo(testo_completo):
    
    pattern_articolo = r"(Art(?:icolo)?\.?\s+\d+[^\n]*)\n"
    matches = list(re.finditer(pattern_articolo, testo_completo))

    if not matches:
        return []

    articoli = []
    for i, match in enumerate(matches):
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(testo_completo)
        titolo = match.group(1).strip()
        testo_raw = testo_completo[start:end].strip()
        


        
        righe = testo_raw.splitlines()
        righe_utili = []

        parole_ban = [
            "aggiornamenti all'atto", "atti aggiornati", "atti correlati", "note atto",
            "indice dell'atto", "torna su", "collegamenti veloci", "archivio news",
            "gazzetta ufficiale", "presidenza del consiglio", "camera dei deputati",
            "senato della repubblica", "istituto poligrafico", "esporta", "akoma ntoso",
            "motore federato", "leggi approvate", "progetto", "note legali", "nascondi", "articolo precedente",
            "articolo successivo", "cookie policy", "contattaci", "normattiva", "note legali"
        ]

        for riga in righe:
            riga_clean = riga.strip().lower()
            if any(p in riga_clean for p in parole_ban):
                break  
            if riga.strip():
                righe_utili.append(riga.strip())

        testo_pulito = "\n".join(righe_utili).strip()
        

        
        if testo_pulito.lower().startswith(titolo.lower()):
            testo_pulito = testo_pulito[len(titolo):].strip()

        articoli.append({
            "numero": titolo,
            "testo": testo_pulito
        })

    return articoli

def carica_articoli_procedurali_da_csv(url, cartella_output):
    nome_file = url.split(";")[0].split(":")[-1].replace("/", "_")
    path_csv = os.path.join(cartella_output, f"articoli_{nome_file}.csv")

    if not os.path.exists(path_csv):
        print(f"⚠️ File CSV articoli non trovato per {url}")
        return []

    df = pd.read_csv(path_csv)
    if "numero" not in df.columns:
        print(f" File {path_csv} non contiene la colonna 'numero'")
        return []

    articoli = df["numero"].dropna().astype(str).tolist()
    return articoli

async def clicca_articolo_da_sidebar(page, numero_articolo: str) -> bool:
    print(f" Cerco e clicco articolo {numero_articolo}...")

    numero_norm = numero_articolo.strip().lower()

    # Trova TUTTI i link nella sidebar
    links = await page.locator("a").all()

    for link in links:
        try:
            testo = (await link.inner_text()).strip().lower()
            testo_norm = re.sub(r"[^\dab]", "", testo) 

            #normlizzazione del numero da cercare
            numero_cercato = re.sub(r"[^\dab]", "", numero_norm)

            if testo_norm == numero_cercato or testo_norm == f"art{numero_cercato}":
                await link.click()
                print(f"Cliccato articolo con testo: '{testo}'")
                return True
        except Exception:
            continue

    print(f" Articolo {numero_articolo} non cliccabile (match esatto fallito)")
    return False

def normalizza_articolo(s: str) -> str:
    return re.sub(r"\W+", "", s.lower().replace("articolo", "").replace("art.", "").strip())



async def process_norma(playwright, url):
    browser = await playwright.chromium.launch()
    page = await browser.new_page()
    try:
        print(f" Apro: {url}")
        await page.goto(url, timeout=60000)
        await page.wait_for_load_state("networkidle")
        nome_file = url.split(";")[0].split(":")[-1].replace("/", "_")

        
        articoli_procedurali = []
        indice_btn = page.locator("a", has_text="Indice dell'atto")
        ha_indice = False

        if await indice_btn.count() > 0:
            try:
                if await indice_btn.first.is_visible():
                    await indice_btn.first.scroll_into_view_if_needed()
                    await indice_btn.first.click(timeout=10000)
                    await page.wait_for_timeout(1500)
                    ha_indice = True
                else:
                    print(" Il bottone 'Indice dell’atto' è presente ma nascosto.")
            except Exception as e:
                print(f" Errore nel cliccare 'Indice dell’atto': {e}")

        #  Analisi dell'indice con LLM
        if ha_indice:
            elementi = await page.query_selector_all("div.modal-body li")
            righe_indice = []
            for el in elementi:
                link = await el.query_selector("a")
                if not link:
                    continue
                numero = await link.inner_text()
                full_text = await el.inner_text()
                titolo = full_text.replace(numero, "", 1).strip(" -–:.\n\t")
                if re.match(r"^\d+[ bis\-]*$", numero) and titolo:
                    righe_indice.append(f"{numero} - {titolo}")

            if not righe_indice:
                print(" Nessun articolo trovato nell'indice.")
                return

            prompt = genera_prompt_etichettatura("\n".join(righe_indice))
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Etichetta ogni articolo come [procedura] o [no]."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0
            )
            testo_risposta = response.choices[0].message.content.strip()
            for riga in testo_risposta.split("\n"):
                match = re.match(r"(\d+[ bis\-]*)\s*-\s*(.*?)\s*→\s*\[(procedura|no)\]", riga.strip())
                if match:
                    numero, _, etichetta = match.groups()
                    if etichetta == "procedura":
                        articoli_procedurali.append(numero.strip())

            # Salva la lista degli articoli con procedure
            if articoli_procedurali:
                df = pd.DataFrame({"numero": articoli_procedurali})
                path_csv = os.path.join(CSV_OUTPUT_FOLDER, f"articoli_{nome_file}.csv")
                df.to_csv(path_csv, index=False, encoding="utf-8-sig")
                print(f" Articoli procedurali salvati: {path_csv}")
            else:
                print(" Nessun articolo procedurale rilevato dal LLM.")

        #  Clic sugli articoli 
        articoli_estratti = []
        articoli_visti = set()
        for numero in articoli_procedurali:
            # Normalizza numero 
            numero_pulito = re.match(r"\d+[ ]?(bis)?", numero.strip().lower())
            if not numero_pulito:
                print(f" Numero articolo non valido: {numero}")
                continue
            numero_pulito = numero_pulito.group()

            print(f" Cerco e clicco articolo {numero_pulito}...")

            if await clicca_articolo_da_sidebar(page, numero_pulito):
                await page.wait_for_timeout(1500)
                testo = await page.inner_text("body")
                

                articoli_puliti = debug_pulisci_testo_normativo(testo)

                numero_norm = normalizza_articolo(numero_pulito)

                trovati = []
                for a in articoli_puliti:
                    numero_blocco = normalizza_articolo(a["numero"])
                    if numero_blocco.endswith(numero_norm) and a["numero"] not in articoli_visti:
                        trovati.append(a)

                if trovati:
                    articoli_estratti.extend(trovati)
                    articoli_visti.update(a["numero"] for a in trovati)
                    print(f" Salvato articolo {numero_pulito} ({len(trovati)} blocchi puliti)")
                else:
                    print(f" Nessun blocco corrispondente al numero {numero_pulito}")



        if articoli_estratti:
            output_testi = os.path.join(CSV_OUTPUT_FOLDER, f"testi_cliccati_{nome_file}.csv")
            pd.DataFrame(articoli_estratti).to_csv(output_testi, index=False, encoding="utf-8-sig")
            print(f" Testi articoli cliccati salvati: {output_testi}")
        else:
            print(" Nessun articolo utile da salvare")

        # Fallback se no indice o articoli cliccabili
        if not ha_indice or (ha_indice and not articoli_procedurali):
            print(" Nessun indice disponibile: provo ad analizzare il testo completo...")
            text_body = await page.inner_text("body")
            articoli = debug_pulisci_testo_normativo(text_body)
            if articoli:
                df = pd.DataFrame(articoli)
                path_fallback = os.path.join(CSV_OUTPUT_FOLDER, f"testi_articoli_fallback_{nome_file}.csv")
                df.to_csv(path_fallback, index=False, encoding="utf-8-sig")
                print(f" Articoli estratti e puliti salvati: {path_fallback}")
            else:
                print(" Nessun articolo rilevato nel testo pulito.")

    except Exception as e:
        print(f" Errore su {url}: {e}")
    finally:
        await browser.close()

async def main():
    df = pd.read_csv(CSV_INPUT)
    urls = df["NormLink"].dropna().tolist()
    urls = [u for u in urls if isinstance(u, str) and u.startswith("http")]
    async with async_playwright() as p:
        for url in urls:
            await process_norma(p, url)

if __name__ == "__main__":
    asyncio.run(main())

async def main():
    df = pd.read_csv(CSV_INPUT)
    urls = df["NormLink"].dropna().tolist()
    
    urls = [u for u in urls if isinstance(u, str) and u.startswith("http")]
    async with async_playwright() as p:
        for url in urls:
            await process_norma(p, url)

if __name__ == "__main__":
    asyncio.run(main())
