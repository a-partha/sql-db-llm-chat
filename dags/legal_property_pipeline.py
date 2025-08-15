import os
import re
import json
from datetime import datetime
from airflow.decorators import dag, task

# Optional: raise timeout via env var (takes effect on next Airflow start)
os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = "60"

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["vector_search"],
)
def legal_property_pipeline():
    """
    Build dbt models, create a sampled vector index for semantic QA,
    and answer queries with an LLM router.
    """

    @task
    def build_dbt_models() -> None:
        import os
        dbt_path = "/opt/airflow/dbt"
        os.system(f"cd {dbt_path} && dbt run --profiles-dir {dbt_path}")

    @task
    def build_vector_index() -> None:
        import duckdb
        import pandas as pd
        import joblib
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.neighbors import NearestNeighbors
        import os

        db_path = "/opt/airflow/data/legal_demo.duckdb"
        conn = duckdb.connect(db_path)
        df = conn.execute("SELECT * FROM acris_clean").fetch_df()

        df = df.sample(n=1000, random_state=42).reset_index(drop=True)

        borough_map = {
            "1": "Manhattan", "2": "Bronx", "3": "Brooklyn",
            "4": "Queens", "5": "Staten Island"
        }

        def row_to_text(row):
            parts = [
                f"Document ID {row['document_id']}",
                f"Record type {row['record_type']}",
                f"Borough {borough_map.get(str(row['borough']), row['borough'])}",
                f"Block {row['block']}",
                f"Lot {row['lot']}",
                f"Property type {row.get('property_type', '')}",
                f"Street number {row.get('street_number', '')}",
                f"Street name {row.get('street_name', '')}",
                f"Unit {row.get('unit', '')}",
                f"Good through date {row.get('good_through_date', '')}",
            ]
            return ", ".join(p for p in parts if str(p).strip())

        df["text"] = df.apply(row_to_text, axis=1)

        vectorizer = TfidfVectorizer(stop_words="english", max_features=5000)
        X = vectorizer.fit_transform(df["text"])

        nn_model = NearestNeighbors(n_neighbors=50, metric="cosine")
        nn_model.fit(X)

        os.makedirs("/opt/airflow/artifacts", exist_ok=True)
        joblib.dump(vectorizer, "/opt/airflow/artifacts/vectorizer.pkl")
        joblib.dump(nn_model, "/opt/airflow/artifacts/nn_model.pkl")
        joblib.dump(df, "/opt/airflow/artifacts/acris_df.pkl")

    @task
    def ask_gemini_with_vector_context(**context) -> str:
        import os
        import re
        import json
        import joblib
        import google.generativeai as genai
        import duckdb

        dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
        prompt = (dag_run_conf.get("prompt") or "").strip()

        def parse_top_k(v, default=50):
            try:
                k = int(v)
                return k if k > 0 else default
            except (TypeError, ValueError):
                return default

        top_k = parse_top_k(dag_run_conf.get("top_k"))

        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY is missing")
        genai.configure(api_key=api_key)

        router_system = (
            "You are a routing assistant. Choose exactly one tool:\n"
            " - sql_count_full: dataset-wide total rows\n"
            " - sql_agg_full_distinct: count distinct values for a single column\n"
            " - semantic: use vector context + LLM\n"
            "Return strict JSON..."
        )

        few_shots = [
            {
                "q": "How many entries are in the dataset?",
                "a": {"tool": "sql_count_full", "arguments": {}, "confidence": 0.95, "reason": "dataset size"},
            },
            {
                "q": "How many boroughs are there?",
                "a": {"tool": "sql_agg_full_distinct", "arguments": {"column": "borough"}, "confidence": 0.9, "reason": "distinct boroughs"},
            },
            {
                "q": "Summarize common record types you see.",
                "a": {"tool": "semantic", "arguments": {}, "confidence": 0.8, "reason": "textual synthesis"},
            },
        ]

        routing_prompt = (
            router_system
            + "\n\nExamples:\n"
            + "\n".join([f"User: {ex['q']}\nReturn: {json.dumps(ex['a'])}" for ex in few_shots])
            + f"\n\nUser: {prompt}\nReturn:"
        )

        try:
            router_model = genai.GenerativeModel("gemini-1.5-flash")
            r = router_model.generate_content(routing_prompt)
            raw = getattr(r, "text", str(r)) or ""
            cleaned = raw.strip().strip("`")
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            router_out = {}
            if start != -1 and end != -1 and end > start:
                router_out = json.loads(cleaned[start: end + 1])
        except Exception:
            router_out = {}

        tool = (router_out.get("tool") or "").strip()
        arguments = router_out.get("arguments") or {}
        confidence = router_out.get("confidence")

        lower = prompt.lower()
        if not tool:
            if re.search(r"\b(total number|dataset size|how many (entries|rows|records|documents))\b", lower):
                tool = "sql_count_full"
            elif re.search(r"\bhow many boroughs?\b", lower):
                tool = "sql_agg_full_distinct"
                arguments = {"column": "borough"}
            else:
                tool = "semantic"

        ti = context["ti"]

        if tool == "sql_count_full":
            conn = duckdb.connect("/opt/airflow/data/legal_demo.duckdb")
            total = conn.execute("SELECT COUNT(*) FROM acris_clean").fetchone()[0]
            answer = f"The dataset has {total:,} entries."
            ctx = "Answered from full table: SELECT COUNT(*) FROM acris_clean"
            ti.xcom_push(key="answer", value=answer)
            ti.xcom_push(key="context", value=ctx)
            return answer

        if tool == "sql_agg_full_distinct":
            safe_columns = {"borough", "record_type"}
            col = (arguments.get("column") or "").strip().lower()
            if col in {"boroughs", "boro", "boros"}:
                col = "borough"
            if col not in safe_columns:
                col = "borough"
            conn = duckdb.connect("/opt/airflow/data/legal_demo.duckdb")
            n_distinct = conn.execute(f"SELECT COUNT(DISTINCT {col}) FROM acris_clean").fetchone()[0]
            answer = f"There are {n_distinct} distinct {col}(s) in the dataset."
            ctx = f"Answered from full table: SELECT COUNT(DISTINCT {col}) FROM acris_clean"
            ti.xcom_push(key="answer", value=answer)
            ti.xcom_push(key="context", value=ctx)
            return answer

        vectorizer = joblib.load("/opt/airflow/artifacts/vectorizer.pkl")
        nn_model = joblib.load("/opt/airflow/artifacts/nn_model.pkl")
        df = joblib.load("/opt/airflow/artifacts/acris_df.pkl")

        query_vec = vectorizer.transform([prompt])
        n_neighbors = max(1, min(top_k, len(df)))
        _, idxs = nn_model.kneighbors(query_vec, n_neighbors=n_neighbors)
        idxs = idxs.flatten()
        context_docs = "\n".join(df.iloc[idxs]["text"].tolist())

        qa_model = genai.GenerativeModel("gemini-1.5-flash")
        message = (
            "You are given the following context from a property deeds dataset:\n\n"
            f"{context_docs}\n\n"
            f"Question: {prompt}\n\nAnswer:"
        )
        try:
            response = qa_model.generate_content(message)
            answer = getattr(response, "text", str(response))
        except Exception as exc:
            answer = f"Error generating answer: {exc}"

        ti.xcom_push(key="answer", value=answer)
        ti.xcom_push(key="context", value=context_docs)
        return answer

    build_dbt_models() >> build_vector_index() >> ask_gemini_with_vector_context()


legal_property_pipeline()

