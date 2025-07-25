import pandas as pd
import re
import json
from openai import OpenAI

client = OpenAI(api_key="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")

input_excel = "C:\\Users\\39345\\Desktop\\procedimenti_uniti.xlsx"
output_csv = "C:\\Users\\39345\\Desktop\\norme_link.csv"


df = pd.read_excel(input_excel)


regex_pattern = re.compile(
    r"(legge|l\.|decreto legislativo|dlgs|d\.lgs|decreto legge|d\.l|dpr|d\.p\.r)[^\d]*(\d+)[^\d]*(\d{4})",
    re.IGNORECASE
)


mapping_tipo = {
    "l.": "legge",
    "legge": "legge",
    "dlgs": "decreto.legislativo",
    "d.lgs": "decreto.legislativo",
    "decreto legislativo": "decreto.legislativo",
    "decreto legge": "decreto.legge",
    "d.l": "decreto.legge",
    "dpr": "decreto.presidente.repubblica",
    "d.p.r": "decreto.presidente.repubblica"
}


def get_normativa_info(normativa):
    if pd.isna(normativa) or normativa.strip().lower() in ["n/d", "n/a", "normativa non specificata"]:
        return {
            "found": False,
            "tipo": "",
            "data": "",
            "numero": "",
            "urn": "",
            "link": "Nessuna normativa specificata",
            "note": "Normativa assente"
        }

    text = normativa.strip().lower()

    match = regex_pattern.search(text)
    if match:
        tipo_raw = match.group(1).lower()
        numero = match.group(2)
        anno = match.group(3)
        tipo = mapping_tipo.get(tipo_raw, tipo_raw)
        data = f"{anno}-01-01"
        tipi_validi = [
            "legge",
            "decreto.legislativo",
            "decreto.legge",
            "decreto.presidente.repubblica",
            "decreto.ministeriale"
        ]
        if tipo in tipi_validi:
            urn = f"urn:nir:stato:{tipo}:{data};{numero}"
            link = f"https://www.normattiva.it/uri-res/N2Ls?{urn}"
            return {
                "found": True,
                "tipo": tipo,
                "data": data,
                "numero": numero,
                "urn": urn,
                "link": link,
                "note": "Estratto con regex"
            }
        else:
            return {
                "found": False,
                "tipo": tipo,
                "data": data,
                "numero": numero,
                "urn": "",
                "link": "Non disponibile su Normattiva (atto locale o non supportato)",
                "note": "Atto non nazionale"
            }

    
    prompt = f"""
Sei un assistente esperto di diritto italiano.
Ricevi questo riferimento normativo:
"{normativa}"

Estrai SOLO queste informazioni e restituisci un JSON con le chiavi seguenti:

{{
 "found": true/false,
 "tipo": "...",
 "data": "...",
 "numero": "...",
 "note": "Eventuali note"
}}

Esempio:
{{
 "found": true,
 "tipo": "legge",
 "data": "1990-08-07",
 "numero": "241",
 "note": ""
}}

Se non trovi informazioni, metti "found": false e spiega in "note".
"""
    completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "Sei un assistente legale esperto di normativa italiana."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )

    response_text = completion.choices[0].message.content
    print(" Risposta modello:\n", response_text)

    try:
        result = json.loads(response_text)
    except json.JSONDecodeError:
        return {
            "found": False,
            "tipo": "",
            "data": "",
            "numero": "",
            "urn": "",
            "link": "Errore parsing JSON",
            "note": response_text
        }

    if result.get("found"):
        tipo = result["tipo"].strip().lower()
        tipi_validi = [
            "legge",
            "decreto.legislativo",
            "decreto.legge",
            "decreto.presidente.repubblica",
            "decreto.ministeriale"
        ]
        if tipo in tipi_validi:
            urn = f"urn:nir:stato:{tipo}:{result['data']};{result['numero']}"
            link = f"https://www.normattiva.it/uri-res/N2Ls?{urn}"
            result["urn"] = urn
            result["link"] = link
        else:
            result["urn"] = ""
            result["link"] = "Non disponibile su Normattiva (atto locale o non supportato)"
    else:
        result["urn"] = ""
        result["link"] = "Normativa non trovata"

    return result

# Applica la funzione
results = df["Normativa"].apply(get_normativa_info)

# Aggiungi colonne
df["NormFound"] = results.apply(lambda x: x.get("found", False))
df["NormTipo"] = results.apply(lambda x: x.get("tipo", ""))
df["NormData"] = results.apply(lambda x: x.get("data", ""))
df["NormNumero"] = results.apply(lambda x: x.get("numero", ""))
df["NormURN"] = results.apply(lambda x: x.get("urn", ""))
df["NormLink"] = results.apply(lambda x: x.get("link", ""))
df["NormNote"] = results.apply(lambda x: x.get("note", ""))

# Salva CSV
df.to_csv(output_csv, index=False, encoding="utf-8")

print(f" File creato: {output_csv}")
