import pandas as pd
from openai import OpenAI

#  API Key
client = OpenAI(api_key="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")

#  Percorsi file
INPUT_CSV = "/Users/michelemazzone/Desktop/tabella_finale_mirata.csv"
OUTPUT_CSV = "procedimenti_con_link_normativa.csv"

#  Domini accettati
DOMINI_ACCETTATI = [
    "normattiva.it", "gazzettaufficiale.it", "brocardi.it",
    "altalex.com", "giustizia.it", "camera.it", "senato.it"
]

def is_link_valido(link):
    return any(dominio in link for dominio in DOMINI_ACCETTATI)

#  Estrazione link normativa in batch
def ottieni_link_normattiva_da_llm(riferimenti, batch_size=5):
    links = []
    for i in range(0, len(riferimenti), batch_size):
        batch = riferimenti[i:i + batch_size]
        prompt = (
            "Per ciascuno dei seguenti riferimenti normativi, fornisci il link diretto alla norma "
            "su Normattiva.it. Se non disponibile, usa una fonte alternativa affidabile come Gazzetta Ufficiale, "
            "Altalex, Brocardi, Giustizia.it, Camera.it o Senato.it. "
            "Rispondi con una riga per ciascun riferimento, nel formato: <numero>) <URL>\n\n"
        )
        for idx, testo in enumerate(batch, 1):
            prompt += f"{idx}) {testo.strip()}\n"

        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            righe = response.choices[0].message.content.strip().split("\n")
            for riga in righe:
                parts = riga.split(" ", 1)
                link = parts[1].strip() if len(parts) > 1 and parts[1].startswith("http") else ""
                links.append(link if is_link_valido(link) else "")
        except Exception as e:
            print(f" Errore batch {i}-{i+batch_size}: {e}")
            links.extend([""] * len(batch))
    return links

# ▶ Main
if __name__ == "__main__":
    df = pd.read_csv(INPUT_CSV)
    riferimenti = df["Normativa"].fillna("").tolist()

    link_normativa = ottieni_link_normattiva_da_llm(riferimenti)

    # Correzione lunghezza se necessario
    while len(link_normativa) < len(df):
        link_normativa.append("")

    df["Link normativa"] = link_normativa
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"File aggiornato salvato: {OUTPUT_CSV}")
