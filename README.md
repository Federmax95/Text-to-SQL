# Text-to-SQL

Repository for querying SQLite databases in natural language using Ollama locally.

The project supports: loading the database from the UI, generating `SELECT` queries, and RAG over a local pool.

## Usage Video



https://github.com/user-attachments/assets/5f8f0a35-3b8f-44ef-99ef-0225a684caab



## Quick Overview

| Web entry point | CLI entry point | Database |
|---|---|---|
| `python app/api.py` | `python app/services/ask.py` | SQLite (`.db`, `.sqlite`, `.sqlite3`) |

UI web: `http://localhost:8000`


## Requirements

- Python 3.10+
- Ollama installed and running at `http://localhost:11434`
- A model available, for example `qwen2.5-coder`

Install dependencies:

```bash
pip install -r requirements.txt
```

Start Ollama in a separate terminal:

```bash
ollama serve
ollama pull qwen2.5-coder
```

## Run

Web API + UI:

```bash
python app/api.py
```

Interactive CLI:

```bash
python app/services/ask.py
```

## UI Usage

- Upload a SQLite file from the start screen or set the database path manually.
- Supported upload formats: `.sqlite`, `.sqlite3`, `.db`.
- After loading, you can ask questions in natural language and get the SQL query with the results.

## Main Endpoints

- `GET /api/health` service status
- `POST /api/set-db` set a SQLite database from a path
- `POST /api/upload-db` upload a SQLite file from the browser
- `POST /api/ask` NL question -> SQL + results
- `POST /api/save` user feedback for the generated query
- `GET /api/pool` list RAG examples
- `POST /api/pool/execute` execute a `SELECT` query from the pool


## Benchmark on the Gretel.ai Dataset

You can run the benchmark using the Gretel.ai sample dataset (https://huggingface.co/datasets/gretelai/synthetic_text_to_sql) and then aggregate the results with the summary script.

Requirements:

- Python environment with the project dependencies installed:

```bash
pip install -r requirements.txt
```

1) Run the benchmark for the Gretel.ai dataset

The repository includes benchmark scripts in `benchmark/`. To run the tests/benchmarks that evaluate the Gretel.ai dataset, you can launch the specific script:

```bash
# runs the Gretel.ai test/benchmark (folder: benchmark)
python benchmark/test_gretelai_synthetic_text_to_sql.py -q
```

2) Where results are saved

Benchmark tools write result JSON files to a `results/` folder. The summary script `benchmark/summarize_benchmark_results.py` scans a directory recursively and finds all `*.json` files to aggregate them.

1) Extract and aggregate the results

Use `benchmark/summarize_benchmark_results.py` to generate aggregated tables in several formats (MD, CSV, JSON, TEX, PDF). The script supports the `--results-dir` option to specify the folder to scan and `--output` for the destination file.

Examples:

```bash
# Print the Markdown table to stdout
python benchmark/summarize_benchmark_results.py --results-dir results --format md

# Save CSV
python benchmark/summarize_benchmark_results.py --results-dir results --format csv --output results/summary.csv

# Save JSON (useful for further filtering with jq/Python)
python benchmark/summarize_benchmark_results.py --results-dir results --format json --output results/summary.json

# Generate PDF
python benchmark/summarize_benchmark_results.py --results-dir results --format pdf --output results/benchmark_summary.pdf
```

## Docker

`docker-compose.yml` is available.

Run:

```bash
docker-compose up --build
```

Services:

- App: `http://localhost:8000`
- Ollama API: `http://localhost:11434`

