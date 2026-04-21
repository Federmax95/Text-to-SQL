# Northwind Text-to-SQL

Un sistema avanzato basato su AI in grado di interrogare un database relazionale (MySQL) in linguaggio naturale (Text-to-SQL). Questo progetto utilizza Large Language Models in locale sfruttando **Ollama** e tecniche di **RAG (Retrieval-Augmented Generation)** per produrre query SQL accurate partendo da semplici domande in linguaggio umano. Restituisce esclusivamente query in lettura (`SELECT`).

---

## 🌟 Caratteristiche Principali

- **LLM in Locale via API**: Utilizza Ollama garantendo la totale privacy del DB.
- **RAG & Vector Embeddings**: Recupera pattern SQL analoghi da un repository in memoria/disco sfruttando la similarità coseno (`SentenceTransformers`).
- **Schema Auto-iniettato**: Connessione dinamica e on-the-fly tramite adapter Python che passa all'LLM nomi di tabelle, foreign keys e perfino una riga di 'Sample Data' reale per evitare allucinazioni sulle stringhe.
- **Guardrail Semantici & AutoFix**: Pipeline a più livelli. Se il sistema sbaglia alias, l'ordinamento (`ORDER BY`) o le query falliscono, corregge automaticamente e internamente la query prima di mandarla in output all'utente.
- **Architettura Modulare**: Utilizzabile come web application oppure nel terminale.



## 🛠️ Prerequisiti

- **Python 3.10+**
- **Ollama Server** con il modello scelto in `app/core/config.py` (default: `qwen2.5-coder`) in esecuzione su `http://localhost:11434`
- **Database MySQL** accessibile e popolato con lo schema Northwind

### Dipendenze Python

```bash
pip install -r requirements.txt
```

---

## 🏎️ Come Iniziare (Setup Locale senza Docker)

### Passo 1 — Preparare il Database

Prima di avviare l'applicazione, è necessario creare e popolare il database MySQL con i dati sintetici Northwind. Nella cartella `init_db/` sono presenti due script SQL da eseguire **in ordine**:

```bash
# 1. Crea il database
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS northwind;"

# 2. Importa lo schema (tabelle, relazioni, vincoli)
mysql -u root -p northwind < init_db/01-northwind.sql

# 3. Importa i dati sintetici
mysql -u root -p northwind < init_db/02-northwind-data.sql
```


### Passo 2 — Avviare Ollama

Assicurati che il server Ollama sia attivo e che il modello sia stato scaricato:

```bash
# Avvia Ollama (se non è già in esecuzione)
ollama serve

# Scarica il modello (solo la prima volta)
ollama pull qwen2.5-coder
```

### Passo 3 — Avviare l'Applicazione

Scegli la modalità che preferisci:

#### Metodo 1: Interfaccia Web (FastAPI)

```bash
# Dalla radice del progetto
python app/api.py
```

> Il front-end sarà disponibile su [http://localhost:8000](http://localhost:8000).

#### Metodo 2: CLI Interattiva da Terminale

```bash
# Dalla radice del progetto
python app/services/ask.py
```

Da qui puoi digitare domande in linguaggio naturale. Al termine di ogni query corretta, il sistema ti propone di **salvare l'esempio nel Vector Pool (RAG)** per migliorare le risposte future.

---

## ⚠️ [SCONSIGLIATO] Architettura ed Esecuzione con Docker Compose 🐳

Se non vuoi avviare manualmente i servizi, installare `MySQL`, `Ollama` e le librerie Python in locale, puoi utilizzare **Docker Compose**. Questa soluzione crea un ambiente riproducibile, isolato e pronto all'uso connettendo automaticamente tutti i componenti necessari.

L'infrastruttura Docker è composta da **4 servizi (container)** orchestrati:

1. **`db` (MySQL 8.0)**: 
   - Espone la porta **3307** (`localhost:3307`) per evitare conflitti con eventuali database locali già in esecuzione sulla classica 3306.
   - Popola automaticamente il database al primo avvio eseguendo gli script `.sql` posizionati nella cartella `init_db/`.
   - Utilizza un volume dedicato (`mysql_data`) per rendere i dati del database Northwind persistenti tra un riavvio e l'altro.

2. **`ollama` (Server LLM Locale)**:
   - Espone la porta **11434**. È il motore AI che eseguirà in locale le richieste di Text-to-SQL garantendo la privacy dei tuoi dati.
   - Usa un volume (`ollama_data`) per mantenere i pesi del modello salvati su memoria persistente (non dovrai riscaricarli ad ogni avvio).

3. **`ollama-pull-model` (Inizializzatore LLM)**:
   - Container ausiliario "usa-e-getta". Dopo l'avvio del server Ollama, questo container si accende per **scaricare automaticamente e silenziosamente** il modello scelto (di default `qwen2.5-coder`). Al termine del download, si spegne.

4. **`app` (FastAPI Backend + Frontend)**:
   - Il cuore del progetto: il backend server Python che contatta il db e Ollama. 
   - Implementa logiche di retry e attesa: aspetta che il database sia *healthy* (pronto a ricevere query) e che Ollama sia online.
   - Serve l'interfaccia utente raggiungibile tramite browser.
   - Espone tutto sulla porta **8000**.

### 🚀 Come Avviare il Progetto con Docker

1. Assicurati di aver inserito il file di esportazione `.sql` (il dump dei dati, es. `northwind.sql`) nella cartella `init_db/`. Le tabelle verranno importate in automatico.
2. Da terminale, sempre posizionandoti nella radice del progetto, esegui:
```bash
docker-compose up --build
```
3. Vai nell'interfaccia! Naviga su: [http://localhost:8000](http://localhost:8000)

> ⏱️ **Nota Iniziale:** Al primissimo avvio ci vorranno alcuni minuti extra (soprattutto in base alla connessione a internet) necessari a Docker per scaricare le immagini di MySQL, Ollama e soprattutto per far estrarre dal container `ollama-pull-model` l'LLM (`qwen2.5-coder` pre-impostato, circa 4.5 GB).

---

## Dashboard Grafana: Query Riuscite vs Fallite

La nuova versione registra eventi in tabella MySQL `query_events` (script `init_db/03-query-events.sql`) quando:

- chiami `/api/ask` (`event_type = 'ask'`)
- salvi feedback da UI (`event_type = 'feedback'`)

### 1) Avvio servizi

Con Docker Compose è incluso anche Grafana:

```bash
docker-compose up --build
```

Grafana sarà disponibile su [http://localhost:3000](http://localhost:3000) con credenziali iniziali:

- user: `admin`
- password: `admin`

### 2) Data source MySQL in Grafana

In Grafana aggiungi Data Source MySQL con:

- Host: `db:3306`
- Database: `northwind`
- User: `root`
- Password: valore di `DB_PASSWORD` (default `root`)

### 3) Query pannello Time Series (success/fail nel tempo)

```sql
SELECT
   UNIX_TIMESTAMP(DATE_FORMAT(created_at, '%Y-%m-%d %H:00:00')) AS time,
   SUM(CASE WHEN is_success = 1 THEN 1 ELSE 0 END) AS succeeded,
   SUM(CASE WHEN is_success = 0 THEN 1 ELSE 0 END) AS failed
FROM query_events
WHERE $__timeFilter(created_at)
   AND event_type = 'ask'
GROUP BY 1
ORDER BY 1;
```

### 4) Query pannello Pie Chart (totale success/fail)

```sql
SELECT
   CASE WHEN is_success = 1 THEN 'Riuscite' ELSE 'Fallite' END AS metric,
   COUNT(*) AS value
FROM query_events
WHERE event_type = 'ask'
GROUP BY is_success;
```

### Nota importante

Se il volume MySQL esiste già, i nuovi script in `init_db/` non vengono rieseguiti automaticamente. In quel caso crea la tabella manualmente:

```sql
SOURCE /docker-entrypoint-initdb.d/03-query-events.sql;
```
