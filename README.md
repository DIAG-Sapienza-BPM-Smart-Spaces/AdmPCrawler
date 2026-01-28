# Crawl4PA

Crawl4PA is a Python-based crawler and scraper designed to automatically extract **administrative procedures** from official Italian Public Administration websites, with a primary focus on municipalities.

The tool navigates institutional websites, collects relevant pages and documents (HTML and PDF), and produces a structured CSV dataset describing administrative procedures, including descriptions, legal bases, and time references.

---

## Features

- **Automated Website Crawling**: Discovers procedure-related sections starting from municipal homepages  
- **Administrative Transparency Navigation**: Targets sections such as *Amministrazione Trasparente* and *Attività e procedimenti*  
- **Multi-format Extraction**: Supports both HTML pages and PDF documents (including scanned PDFs via OCR)  
- **Semantic Information Extraction**: Uses heuristic rules and Large Language Models (LLMs) to identify procedures  
- **Structured Output**: Generates CSV files suitable for analysis and downstream processing  

---

## Repository Structure

Crawl4PA/
```
│
├── crawl4pa.py # Main crawler and extraction script
├── requirements.txt # Python dependencies
├── README.md # Project documentation
│
├── results/                     # Evaluation results
│   └── evaluation.xlsx          # Evaluation of section identification, procedure extraction, and legal enrichment
│
└── keywords/ # Keyword lists used for discovery and filtering
└── keywords.txt
```

---

## Installation

Clone the repository:

```bash
git clone https://github.com/.../Crawl4PA.git
cd Crawl4PA
```
Install the required packages:

```bash
pip install -r requirements.txt
```
Note: Some dependencies (e.g. Playwright, Tesseract OCR) require additional system setup.

Usage
Run the Crawler

Before running the script, configure the following parameters inside crawl4pa.py:
- Path to the file containing municipality websites
- Path to the keyword list
- Output CSV path
- OpenAI API key (required for LLM-based extraction)

Then run:
```
python crawl4pa.py
```
