# Text-to-SQL

Repository per interrogare database SQLite con linguaggio naturale usando Ollama in locale.

La versione consente: caricamento o selezione del database da UI, generazione dataset sintetico con conseguente caricamento in memoria, generazione di query `SELECT`, RAG su pool locale e creazione opzionale di dataset sintetico.

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
- Se il database lo consente, puoi anche generare un dataset sintetico e ricaricarlo dalla stessa interfaccia.

## Dataset Sintetico

La UI include il pulsante `Genera Dataset Sintetico`.

Il flusso esegue:

1. generazione del dataset sintetico a partire dal DB corrente;
2. validazione con Pandera e SDMetrics;
3. salvataggio del DB sintetico e caricamento automatico in UI.

La percentuale di accuratezza SDV viene mostrata al termine del caricamento del dataset sintetico.

## Endpoint Principali

- `GET /api/health` stato servizio
- `POST /api/set-db` imposta un database SQLite da path
- `POST /api/upload-db` carica un file SQLite dal browser
- `POST /api/generate-synthetic-db` genera il dataset sintetico
- `POST /api/set-synthetic-db` carica il dataset sintetico generato
- `POST /api/ask` domanda NL -> SQL + risultati
- `POST /api/save` feedback utente per la query generata
- `GET /api/pool` elenco esempi RAG
- `POST /api/pool/execute` esegue una query `SELECT` dal pool

## Docker

Per la V2 è disponibile `docker-compose.yml`.

Avvio:

```bash
docker-compose up --build
```

Servizi:

- App: `http://localhost:8000`
- Ollama API: `http://localhost:11434`



- V1: schema fisso Northwind su MySQL
- V2: schema variabile, scelto dall'utente su SQLite
