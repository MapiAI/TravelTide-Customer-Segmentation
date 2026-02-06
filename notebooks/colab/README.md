## 📒 Notes on Notebook Execution (Databricks vs Colab)

The analytical workflow of this project is implemented as a sequence of notebooks.

All notebooks **except for the exploratory SQL sections of Notebook 02**
are fully compatible with **Google Colab** and rely on either:
- lightweight data inspection, or
- exported CSV datasets.

### Notebook 01 – Business and Data Understanding

**Notebook 01** is fully compatible with **Google Colab**.
It contains business context, data understanding, and analytical framing,
and does not require access to large raw tables or distributed execution.

### Notebook 02 – Data Preparation & SQL Exploration

The notebook  
**`02_data_preparation_sql_exploration`**  
performs large-scale **SQL-based data understanding and validation**
directly on the raw TravelTide database.

Due to the **size of the underlying tables** (e.g. the `sessions` table contains more
than 5 million records), the full exploratory SQL workflow is designed to run in
a distributed environment (**Databricks + Spark + JDBC**).

To ensure reproducibility in lightweight environments (such as Google Colab),
the execution logic of this notebook is intentionally split:

- **At the beginning of the notebook**, a dedicated section  
  **“Session-Level Dataset Materialization”**  
  executes a single, consolidated SQL query that:
  - applies the selected cohort and temporal rules,
  - reconstructs consistent booking–cancellation lifecycles,
  - and materializes the **session-level dataset** used by all downstream notebooks.

- **The remainder of the notebook** serves a **descriptive and documentary purpose**:
  it records the exploratory SQL analysis used to understand the data model,
  validate lifecycle assumptions, and justify filtering and aggregation choices.
  These cells are not required to be re-executed in Colab.

For full transparency, **all exploratory SQL queries used in this notebook**
are collected and documented in:

- 📄 Session-level SQL exploration: [session_level_data_understanding.sql](scripts/sql/session_level_data_understanding.sql)

When running the project in Colab, **only the initial materialization step**
needs to be executed.

### Notebooks 03 and Beyond

All notebooks from **Notebook 03 onward** (session cleaning, feature engineering,
segmentation, clustering, and evaluation) are fully compatible with **Google Colab**
and operate exclusively on the exported CSV datasets.

This design reflects a deliberate architectural choice:
- heavy data validation in a distributed environment,
- followed by portable, reproducible analytics and modeling notebooks.
