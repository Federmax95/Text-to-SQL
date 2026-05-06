import os
import sys
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd


model = SentenceTransformer('all-MiniLM-L6-v2')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(BASE_DIR))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from app.core.config import DB_CONFIG
from app.services.schema_adapter import NorthwindSchemaAdapter
from app.core.config import DB_CONFIG,DB_CONFIG2


def column_similarity(col_real, col_synth, model):

    emb_real = model.encode(col_real.astype(str).tolist())
    emb_synth = model.encode(col_synth.astype(str).tolist())


    sim_matrix = cosine_similarity(emb_real, emb_synth)
    return sim_matrix.max(axis=1).mean()



if __name__ == "__main__":
    adapter = NorthwindSchemaAdapter(DB_CONFIG=DB_CONFIG)
    adapter2 = NorthwindSchemaAdapter(DB_CONFIG=DB_CONFIG2)
    for table in adapter.extract_tables():
        print("Tabella: ",table)
        try:
            cols_r, rows_r = adapter.extract_data(table)
            cols_s, rows_s = adapter2.extract_data(table)
            df_real = pd.DataFrame(rows_r, columns=cols_r)
            df_synth = pd.DataFrame(rows_s, columns=cols_s)

            common_cols = set(df_real.columns) & set(df_synth.columns)

            for col in common_cols:
                try:
                    score = column_similarity(df_real[col], df_synth[col], model)
                    print(f"{col}: {score:.3f}")
                except Exception as e:
                    print(f"{col}: errore ({e})")

        except:
            print("❌ errore lettura dati")
            continue