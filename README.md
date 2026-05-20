# Text-to-SQL

Repository per interrogare database SQLite con linguaggio naturale usando Ollama in locale.

La versione consente: caricamento o selezione del database da UI, generazione di query `SELECT`, RAG su pool locale.

## Video di utilizzo



https://github.com/user-attachments/assets/5f8f0a35-3b8f-44ef-99ef-0225a684caab



## Panoramica Rapida

| Entry point web | Entry point CLI | Database |
|---|---|---|
| `python app/api2.py` | `python app/services/ask2.py` | SQLite (`.db`, `.sqlite`, `.sqlite3`) |

UI web: `http://localhost:8000`


## Prerequisiti

- Python 3.10+
- Ollama installato e attivo su `http://localhost:11434`
- Modello disponibile, ad esempio `qwen2.5-coder`

Installa le dipendenze:

```bash
pip install -r requirements.txt
```

Avvia Ollama in un terminale separato:

```bash
ollama serve
ollama pull qwen2.5-coder
```

## Avvio

Web API + UI:

```bash
python app/api.py
```

CLI interattiva:

```bash
python app/services/ask.py
```

## Uso della UI

- Carica un file SQLite dalla schermata iniziale oppure imposta manualmente il path del database.
- Formati supportati in upload: `.sqlite`, `.sqlite3`, `.db`.
- Dopo il caricamento, puoi inviare domande in linguaggio naturale e ottenere la query SQL con i risultati.
-- (La generazione di dataset sintetici è stata rimossa dall'interfaccia.)

## Endpoint Principali

- `GET /api/health` stato servizio
- `POST /api/set-db` imposta un database SQLite da path
- `POST /api/upload-db` carica un file SQLite dal browser
- `POST /api/ask` domanda NL -> SQL + risultati
- `POST /api/save` feedback utente per la query generata
- `GET /api/pool` elenco esempi RAG
- `POST /api/pool/execute` esegue una query `SELECT` dal pool


## Benchmark sul dataset Gretel.ai

È possibile eseguire il benchmark usando il dataset di esempio fornito per Gretel.ai (`data/gretelai_preview_200.csv`) e poi aggregare i risultati con lo script di riepilogo.

Prerequisiti:

- Ambiente Python con le dipendenze del progetto installate:

```bash
pip install -r requirements.txt
```

1) Eseguire il benchmark per il dataset Gretel.ai

Nel repository sono presenti script di benchmark in `benchmark/`. Per eseguire i test/benchmark che valutano il dataset Gretel.ai puoi lanciare lo script specifico o usare `pytest` sul file dedicato. Esempio:

```bash
# esegue il test/benchmark per Gretel.ai (cartella: benchmark)
pytest benchmark/test_gretelai_synthetic_text_to_sql.py -q
```

1) Dove vengono salvati i risultati

I tool di benchmark scrivono file JSON di risultato in una cartella `results/` (può variare a seconda dello script usato). Lo script di riepilogo `benchmark/summarize_benchmark_results.py` scansiona ricorsivamente una directory e trova tutti i file `*.json` per aggregarli.

3) Estrarre e aggregare i risultati

Usa `benchmark/summarize_benchmark_results.py` per generare tabelle aggregate in vari formati (MD, CSV, JSON, TEX, PDF). Lo script supporta l'opzione `--results-dir` per indicare la cartella da scansionare e `--output` per il file di destinazione.

Esempi:

```bash
# Stampare la tabella Markdown su stdout
python benchmark/summarize_benchmark_results.py --results-dir results --format md

# Salvare CSV
python benchmark/summarize_benchmark_results.py --results-dir results --format csv --output results/summary.csv

# Salvare JSON (utile per ulteriori filtraggi con jq/Python)
python benchmark/summarize_benchmark_results.py --results-dir results --format json --output results/summary.json

# Generare PDF
python benchmark/summarize_benchmark_results.py --results-dir results --format pdf --output results/benchmark_summary.pdf
```

## Docker

Per la V2 è disponibile `docker-compose.yml`.

Avvio:

```bash
docker-compose up --build
```

Servizi:

- App: `http://localhost:8000`
- Ollama API: `http://localhost:11434`

