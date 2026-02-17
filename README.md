![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![SQL](https://img.shields.io/badge/SQL-PostgreSQL-blue?logo=postgresql)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Analysis-purple?logo=pandas)
![Scikit-learn](https://img.shields.io/badge/Scikit--Learn-Clustering-orange?logo=scikit-learn)
![Spark](https://img.shields.io/badge/Apache%20Spark-Data%20Processing-orange?logo=apachespark)
![CRISP-DM](https://img.shields.io/badge/Methodology-CRISP--DM-success)
![Status](https://img.shields.io/badge/Project-Academic%20Case%20Study-lightgrey)

# ✈️ TravelTide – Customer Segmentation & Perk Strategy

This repository contains an end-to-end customer segmentation project developed as part of a **MasterSchool Data Science program**.

The objective is to design a **behavior-driven, risk-aware perk strategy** for a travel booking platform (TravelTide), combining interpretable rule-based logic with data-driven validation.

---

## 📌 Project Overview

TravelTide is a young travel-booking platform operating in a short historical window.

The business goal is to increase rewards program sign-ups by emphasizing the perk each customer is most likely to value in invitation emails.

Rather than offering generic discounts, the objective is to address real behavioral frictions in travel planning through personalized incentives.

This project:

- builds a **user-level analytical dataset** from session-level behavior,
- defines a **transparent segmentation framework** based on behavioral dimensions,
- assigns **exactly one perk per user** through explicit eligibility and priority rules,
- and validates the segmentation logic using **unsupervised clustering**.

The final output of the project is a user-level dataset ready for activation,
where each user is assigned exactly one perk based on interpretable behavioral logic.

---

## 🎯 Alignment with Campaign Design

The analysis is strictly aligned with the original campaign objective.

The business proposed the following perks:

- No Cancellation Fees  
- Free Checked Bag  
- 1 Free Hotel Night with Flight  
- Exclusive Discount  
- Free Hotel Meal  

Behavioral segmentation and feature engineering were designed to evaluate whether distinct user groups naturally aligned with these incentives.

The multi-layer segmentation framework provided strong coverage across user behavior patterns, and no additional perks were required to meaningfully differentiate customers.

This indicates that the proposed reward structure is behaviorally well-calibrated to the observed customer base.

---

## 🧭 Methodology (CRISP-DM)

The project follows the **CRISP-DM framework**, adapted to a decision-oriented analytics context:

1. **Business Understanding**  
   Define business objectives, constraints, and decision levers (perks).

2. **Data Understanding**  
   Explore raw tables (users, sessions, flights, hotels) and validate schema, granularity, and lifecycle logic.

3. **Data Preparation**  
   Build session-level and user-level features capturing engagement, booking behavior, spend, risk, and trip structure.

4. **Modeling**  
   - Rule-based behavioral segmentation  
   - Perk eligibility and priority-based assignment  
   - Unsupervised clustering (validation only)

5. **Evaluation**  
   Assess alignment between perks, behavioral clusters, risk exposure, and economic upside.

6. **Deployment / Recommendations**  
   Translate findings into actionable, risk-controlled business recommendations.

Each CRISP-DM phase is implemented in a dedicated notebook, resulting in a
multi-notebook analytical pipeline designed to mirror a real-world data science workflow.

---

## 🧠 Behavioral Segmentation Framework

Segmentation is **not driven by clustering**.

Users are segmented through a **rule-based, interpretable framework** built around four independent behavioral dimensions:

- **Value** – economic upside (total spend, CLTV)
- **Risk** – booking reliability (cancellation behavior)
- **Trip Complexity** – itinerary structure, duration, distance, group size
- **Engagement** – behavioral intensity (RFM signals)

These dimensions are intentionally orthogonal and reflect real decision frictions in travel planning.

The orthogonality of these dimensions ensures that segmentation is not driven by overlapping signals or redundant metrics, but by distinct behavioral axes capturing independent travel decision frictions.

A lightweight fallback rule guarantees full population coverage while preserving interpretability.

---

## 🎁 Perk Assignment Logic

Perks are treated as **behavioral interventions**, not rewards.

Each user is assigned **exactly one perk** through:

- explicit eligibility rules,
- lifecycle guardrails (e.g. completed trips),
- and a **priority-based decision framework**.

A fallback rule ensures that users who do not meet any primary eligibility pattern still receive a consistent, business‑aligned perk assignment.

Perks include:

- No Cancellation Fees  
- Free Checked Bag  
- 1 Free Hotel Night with Flight  
- Exclusive Discount  
- Free Hotel Meal  

The assignment logic is fully transparent and auditable. 

The framework achieves full population coverage:

- **96.4%** of users assigned via explicit behavioral logic  
- **3.6%** assigned through a controlled fallback mechanism  

This ensures deterministic assignment without leaving edge cases unresolved.

---

## ✅ Validation Checks

A final validation block is implemented at the end of **Notebook 05**, ensuring that the user‑level dataset is complete, consistent, and activation‑ready.

The validation confirms that:

- each user receives **exactly one perk**,  
- no duplicate `user_id` values exist,  
- no null or unexpected perk assignments occur (including fallback users),  
- the fallback share is explicitly monitored.

Together, these controls ensure that the final dataset is deterministic, auditable, and directly usable for campaign activation or experimentation without additional post-processing.

---

## 🔍 Clustering as Validation (Not Decision-Making)

Unsupervised clustering is applied *after* rule-based segmentation and perk assignment.

Its purpose is not to generate segments, but to validate whether meaningful behavioral structure exists in the raw data.

Clustering:

- uses only raw behavioral features,
- excludes engineered tiers, rules, and perk labels,
- does not influence business decisions,
- serves purely as structural validation.

The key question addressed is:

> Does the rule-based segmentation reflect real structure in the underlying behavioral feature space?

K-Means clustering (k=4) yields a Silhouette score of **0.467**, indicating meaningful separation in customer travel behavior.

The resulting clusters naturally reproduce the same core behavioral dimensions  
(Value, Risk, Trip Complexity, Engagement), providing strong ex-post confirmation of the segmentation framework.

Clustering is therefore used to test structural coherence, not to drive decision logic.

---

## 🔍 Segmentation Robustness

The robustness of the framework is supported by:

- Explicit behavioral rule definitions
- Structural validation through unsupervised clustering (Silhouette score: 0.467)
- Full population coverage with controlled fallback
- Deterministic one-perk-per-user assignment
- Clear separation between decision logic and validation logic

Clustering is used to verify the existence of meaningful structure in raw behavioral features, not to drive business decisions.

---

## 📓 Execution Notes (Databricks & Colab)

This project can be executed both in **Databricks** and in **Google Colab**.

All notebooks are Colab-compatible.  
However, **Notebook 02 – Data Understanding & SQL Exploration** interacts directly with large raw database tables (e.g. `sessions` ~5.4M rows).  
For this reason, in Colab it is designed to support a lightweight “quick-run” flow:

- **Run only the first two code cells** to execute the cohort selection + session-level extraction query and **export the resulting dataset to CSV**.
- That exported CSV becomes the input for **Notebook 03**, allowing the rest of the pipeline to run end-to-end in Colab.

The remainder of Notebook 02 is intentionally kept as a **fully documented SQL exploration and validation notebook**: it examines the database structure, relationships, and lifecycle logic (booking vs browsing vs cancellations), providing a detailed rationale for the cohort definition and the session-level dataset design.

For convenience and reproducibility, the SQL queries used in Notebook 02 are also collected here:
- [`scripts/sql/session_level_data_understanding.sql`](scripts/sql/session_level_data_understanding.sql)

> **Performance Note**
> Notebook 02 and Notebook 03 contain extensive SQL exploration and large rendered outputs.
> When viewed directly on GitHub, they may load slowly due to the volume of visualizations and result tables.

---

## 📁 Repository Structure

```text
TravelTide/
│
├── notebooks/               
│   └── *.ipynb              # Databricks notebooks (full analytical pipeline) 
│   └── colab/               # Google Colab–compatible notebooks           
│                            # (Notebook 02 supports a quick-run mode for large SQL queries)
├── data/
│   ├── raw/                 # Raw CSV files extracted from the database
│   ├── intermediate/        # Cleaned and feature-engineered datasets used for segmentation and modeling
│   └── final/               # Final segmentation output with assigned perks (one perk per user)
│
├── scripts/                 # SQL queries and helper scripts
├── reports/                 # PDF reports and presentation slides
│
├── .gitignore
├── LICENSE
└── README.md
└── requirements.txt
```
---
## 📦 Requirements

A minimal `requirements.txt` is included to support reproducibility.

To install dependencies:

```
pip install -r requirements.txt
```

The project uses standard data science libraries such as:

- pandas  
- numpy  
- scikit-learn  
- matplotlib / seaborn  
- plotly  
- scipy  
- python-dotenv
  
---

## 🔐 Environment Variables

Database credentials are **not stored** in the repository.

To run the extraction step, create a `.env` file in the project root:

```
TRAVELTIDE_DB_PASSWORD=your_password_here
```
The `.env` file is excluded via `.gitignore`.

If no credentials are available, the **Colab version** can be reviewed end‑to‑end using pre‑executed outputs.

---

> **Note**  
> The database credentials used in the notebooks belong to a public,
> read-only educational PostgreSQL database provided as part of the MasterSchool program
> and are included solely to ensure reproducibility in an academic context.

