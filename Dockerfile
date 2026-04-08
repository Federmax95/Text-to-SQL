FROM python:3.10-slim

# Imposta la directory di lavoro all'interno del container
WORKDIR /app

# Installa eventuali pacchetti di base per la compilazione (richiesti per alcuni motori o librerie numpy/scipy ecc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia i requisiti e installali per sfrutta la cache dei layer di Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia l'intero progetto all'interno del container
COPY . .

# Imposta alcune variabili d'ambiente di default utili
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Esponi la porta 8000 per l'app FastAPI
EXPOSE 8000

# Avvia Uvicorn (FastAPI)
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
