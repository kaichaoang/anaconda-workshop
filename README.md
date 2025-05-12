### 🔧 Prerequisites

- Python 3.10+
- Poetry (https://python-poetry.org/docs/#installation)

Install dependencies:
```
python -m venv .venv
.venv\Scripts\activate

pip install poetry
poetry install
```


---

### ▶️ Run the Full Pipeline

Run all stages (ingestion, reconciliation, ROR):
```
poetry run python -m src.main --all
```

### 🧪 Testing
```
poetry run pytest
```

### ⚙️ Run ruff check with auto fix
```
poetry run ruff check --fix
```

### ✅ Answers
1. Tools to achieve production level:
    - Production grade database (currently running in sqlite), datasets will expect to be massive
    - Production grade computation power (parallelism, concurrency etc.), expected to have thousands of different funds
    - To store ingestion configs in database rather than constant.

2. Increase scalability for **n** fund reports by:
   - Appending regex to **file_identifier_map** constant to identify the new fund 
   - In the case for new interested columns, append in **columns_to_include**
   - Report generation services will run as per normal since it requires just the extracted fund name and reporting date.

### 💡 Assumptions for ROR
1. Included Equities and Government Bond

### 💡 Assumptions for ROR
1. Assumed Fund MV start and Fund MV end is obtained by **quantity * reference_prices** from master data
2. Included Equities and Government Bond
3. Ignored CASH

---

### 🔹 Run Individual Tasks

Only ingest external fund data:
```
poetry run python -m src.main --ingest
```

Only generate the reconciliation report:
```
poetry run python -m src.main --recon
```
Only generate the rate of return (ROR) report:
```
poetry run python -m src.main --ror
```

