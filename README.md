# ✈️ TravelTide – Customer Segmentation & Perk Strategy

This repository contains an end-to-end customer segmentation project developed as part of a **MasterSchool Data Science program**.

The objective is to design a **behavior-driven, risk-aware perk strategy** for a travel booking platform (TravelTide), combining interpretable rule-based logic with data-driven validation.

---

## 📌 Project Overview

TravelTide is a young travel-booking platform operating in a short historical window.  
The business goal is to improve **conversion and retention** by offering **personalized perks** that address real user frictions rather than generic discounts.

This project:

- builds a **user-level analytical dataset** from session-level behavior,
- defines a **transparent segmentation framework** based on behavioral dimensions,
- assigns **exactly one perk per user** through explicit eligibility and priority rules,
- and validates the segmentation logic using **unsupervised clustering**.

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

---

## 🧠 Behavioral Segmentation Framework

Segmentation is **not driven by clustering**.

Users are segmented through a **rule-based, interpretable framework** built around four independent behavioral dimensions:

- **Value** – economic upside (total spend, CLTV)
- **Risk** – booking reliability (cancellation behavior)
- **Trip Complexity** – itinerary structure, duration, distance, group size
- **Engagement** – behavioral intensity (RFM signals)

These dimensions are intentionally **orthogonal** and reflect real decision frictions in travel planning.

---

## 🎁 Perk Assignment Logic

Perks are treated as **behavioral interventions**, not rewards.

Each user is assigned **exactly one perk** through:

- explicit eligibility rules,
- lifecycle guardrails (e.g. completed trips),
- and a **priority-based decision framework**.

Perks include:

- No Cancellation Fees  
- Free Checked Bag  
- 1 Free Hotel Night with Flight  
- Exclusive Discount  
- Free Hotel Meal  

The assignment logic is fully transparent and auditable.

---

## 🔍 Clustering as Validation (Not Decision-Making)

Unsupervised clustering is applied **after** segmentation and perk assignment.

Clustering:

- uses **only raw behavioral features**,
- excludes all engineered scores, tiers, rules, and perk labels,
- does **not** influence business decisions.

Its purpose is to answer one question:

> *Does the rule-based segmentation reflect real structure in the data?*

The resulting clusters naturally reproduce the same underlying dimensions  
(**Value, Risk, Complexity, Engagement**), providing strong ex-post validation.

**Clustering is used for validation, not for decision-making.**

---

## 📁 Repository Structure

```text
TravelTide/
│
├── notebooks/
│   └── databricks/          # Databricks notebooks (full analytical pipeline)
│
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
