import pandas as pd
import os
from openai import OpenAI
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.styles import ParagraphStyle

# Inizializza API OpenAI
client = OpenAI(api_key="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")

# File input/output
CSV_INPUT = "procedimenti_con_link_normativa.csv"
CSV_OUTPUT = "procedimenti_con_articoli_pdf.csv"
OUTPUT_DIR = "pdf_articoli_llm"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Carica il CSV
df = pd.read_csv(CSV_INPUT)

# Funzione per ottenere articoli da LLM
def ottieni_articoli_da_llm(link):
    prompt = f"""
Agisci come assistente legale esperto. Accedi a questo link Normattiva:
{link}
e restituisci SOLO l'articolo presente in pagina in questo formato (senza intestazioni, introduzioni, o spiegazioni aggiuntive):

Articolo: <numero articolo>
Testo: <testo dell'articolo>

Non aggiungere altro.
"""
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f" Errore GPT su {link}: {e}")
        return ""

# Funzione per creare PDF leggibile con testo che va a capo
def crea_pdf(titolo_attivita, testo_articolo, filepath):
    doc = SimpleDocTemplate(filepath, pagesize=A4,
                            rightMargin=2*cm, leftMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)

    styles = getSampleStyleSheet()
    style_body = ParagraphStyle(name="Body", parent=styles['Normal'], fontSize=11, leading=14, alignment=TA_LEFT)

    elementi = []
    elementi.append(Paragraph(f"<b>{titolo_attivita}</b>", styles["Title"]))
    elementi.append(Spacer(1, 0.5*cm))
    for riga in testo_articolo.split("\n"):
        if riga.strip():
            elementi.append(Paragraph(riga.strip(), style_body))
            elementi.append(Spacer(1, 0.3*cm))

    doc.build(elementi)

# Estrazione e salvataggio
for index, row in df.iterrows():
    url = row.get("Link normativa", "")
    nome_attivita = str(row.get("Nome Attività", f"articolo_{index+1}")).strip().replace("/", "_").replace(" ", "_")[:80]
    filepath = os.path.join(OUTPUT_DIR, f"{nome_attivita}.pdf")

    if isinstance(url, str) and url.startswith("http"):
        contenuto = ottieni_articoli_da_llm(url)
        if contenuto:
            crea_pdf(nome_attivita.replace("_", " "), contenuto, filepath)
            df.at[index, "File PDF"] = filepath
            print(f" Creato PDF: {filepath}")
        else:
            df.at[index, "File PDF"] = ""
    else:
        df.at[index, "File PDF"] = ""

# Salva CSV aggiornato
df.to_csv(CSV_OUTPUT, index=False)
print(f"\n Completato! File aggiornato: {CSV_OUTPUT}")
