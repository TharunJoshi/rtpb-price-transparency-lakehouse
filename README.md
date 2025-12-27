python -m pip --version
python -m pip install pyspark==3.5.0
cd C:\Users\tharu\rtpb-price-transparency-lakehouse
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install pyspark==3.5.0


# RTPB Price Transparency Lakehouse

End-to-end **Real-Time Prescription Benefit (RTPB)** price transparency Lakehouse, inspired by pharmacy benefit manager (PBM) workflows.

This project simulates how an EHR (like Epic) calls a PBM (like Express Scripts) to get:

- Patient-specific **out-of-pocket cost** for a drug  
- **Covered alternatives**
- **Flags** like prior authorization (PA) and step therapy  

and how those RTPB events land in a Lakehouse (Bronze → Silver → Gold) for analytics.

---

## 1. Problem This Project Solves

When doctors prescribe meds, patients often do not know:

- How much they will pay at the pharmacy  
- Whether cheaper covered alternatives exist  
- Whether PA / step therapy delays will occur  

RTPB solves this by returning **real-time price quotes and alternatives** at the point of prescribing.

This repo focuses on the **data engineering side**:

- Ingest RTPB requests + responses (simulated, de-identified)  
- Normalize and model data in **Bronze / Silver / Gold** layers  
- Compute analytics such as:
  - Generic switch rate  
  - Savings from alternatives  
  - Cost variation by plan type (Commercial vs DoD/Tricare)  
  - PA / step therapy flag rates  

---

## 2. Architecture (Conceptual)

```text
EHR (Epic - simulated)
   → RTPB Requests (rtpb_requests)
   → PBM Pricing Logic (simulated formulary + benefit rules)
   → RTPB Responses:
        - price_quotes
        - covered_alternatives
   → Lakehouse:
        - Bronze: raw events
        - Silver: normalized + enriched
        - Gold: analytics KPIs





This repository contains an end-to-end Lakehouse data engineering project that simulates how Real-Time Prescription Benefits (RTPB) work inside large PBMs (Pharmacy Benefit Managers) and health plan systems.

The goal: show out-of-pocket cost transparency at the point-of-care (doctor → patient → pharmacy) and recommend lower-cost covered alternatives in real-time.

📌 Business Problem

When a doctor prescribes a medication, patients do not know:

How much the drug will cost under their insurance

If a cheaper generic is available

Whether Prior Authorization (PA) or Step Therapy is required

If DoD (Military) members get $0 / discounted pricing

RTPB fixes this by generating a real-time cost quote + alternative options before the prescription is sent to the pharmacy.




Data Sources → Bronze (raw) → Silver (normalized + PHI-clean) → Gold (KPIs)


HIPAA defines 18 Protected Health Information (PHI) identifiers, including:
Name, address, DOB, phone, email, SSN, medical record number, plan ID, full-face photos, IP address, biometric identifiers.

RTPB systems should never expose PHI to analytics or downstream AI models.

Therefore, this project automatically removes PHI-equivalent fields in Silver using:

PHI_COLUMN_CANDIDATES = {...}  # 18 HIPAA categories mapped to column names


This simulates real-world production pipelines where analytics, machine learning, and dashboards operate only on de-identified, tokenized data.





| Table                      | Layer                         | Purpose                                                                    |
| -------------------------- | ----------------------------- | -------------------------------------------------------------------------- |
| `dim_member`               | Bronze → Silver (PII cleaned) | Member demographics (de-identified); supports plan lookup; **PHI removed** |
| `dim_plan`                 | Bronze → Silver enrich        | Commercial vs DoD, metal tier, deductible, formulary rules                 |
| `dim_drug`                 | Bronze → Silver enrich        | NDC codes, therapeutic class, generic indicator                            |
| `dim_pharmacy`             | Bronze                        | Retail / mail-order / DoD facilities                                       |
| `dim_prescriber`           | Bronze                        | Prescriber metadata (NPI removed in Silver)                                |
| `fact_rtpb_request`        | Bronze → Silver               | Represents **the real-time request** made when doctor queries pricing      |
| `fact_price_quote`         | Bronze → Silver               | **Main response**: out-of-pocket cost, PA flag, formulary tier             |
| `fact_covered_alternative` | Bronze → Silver               | Returned list of alternative drugs & their cost                            |
| `fact_claim`               | Bronze only                   | Simulates downstream claims used in future ML modeling                     |
| `formulary_rule`           | Bronze                        | Defines plan-drug restrictions, tiering, step therapy                      |
| `pa_request`               | Bronze                        | Captures prior authorization workflow events                               |
| `audit_event`              | Bronze                        | Logging & audit for traceability and governance                            |




Doctor writes prescription
      ↓
EHR sends RTPB request → fact_rtpb_request
      ↓
Pricing engine checks:
    - member plan & deductible → dim_member, dim_plan
    - drug cost & formulary → dim_drug, formulary_rule
      ↓
Cost returned → fact_price_quote
Covered alternatives suggested → fact_covered_alternative
If blocked → PA request created → pa_request
Audit stored → audit_event









## Architecture & Data Model

This repo simulates a Real-Time Prescription Benefit (RTPB) lakehouse similar to what PBMs (e.g., Express Scripts) and EHRs (e.g., Epic) use in production.

### High-level flow

1. **RTPB request** is generated by the EHR when a prescriber enters a drug.
2. The PBM pricing engine returns:
   - A **price quote** for the prescribed drug.
   - A set of **covered alternatives** (generic / preferred brands) with their own prices.
3. All of these events are captured into a data platform for **analytics, audit, and regulatory reporting**.

This project focuses on the *analytics side* of that flow using a Medallion architecture.

### Medallion layers

- **Raw (CSV)** – synthetic input files under `data/sample/`
- **Bronze (Parquet)** – one-to-one copy of raw tables under `data/bronze/`
- **Silver (Parquet)** – cleaned / conformed facts under `data/silver/`
- **Gold (Parquet)** – KPI tables under `data/gold/` (e.g., average OOP cost, generic switch rate)

The same pattern can be moved 1:1 to **Databricks + Delta Lake + DLT**.

### Tables (12-table mini lakehouse)

**Dimensions**

- `dim_member` — synthetic member attributes (`member_id`, `member_sk`, gender, age band, coverage type, segment, region, enrollment dates, etc.)
- `dim_plan` — insurance plan attributes (`plan_id`, line of business, product, metal tier, formulary ID, DoD vs Commercial flag, etc.)
- `dim_drug` — drug reference (`drug_ndc`, `drug_name`, strength, route, therapeutic class, generic / brand flags)
- `dim_pharmacy` — pharmacy attributes (`pharmacy_id`, chain vs independent, retail vs mail order, region, 340B flags)
- `dim_prescriber` — prescriber attributes (`prescriber_id`, NPI, specialty, region, practice type)

**Core facts**

- `fact_rtpb_request` — one row per RTPB request from the EHR (transaction_id, plan_id, drug_ndc, requested quantity / dosage, timestamp, channel)
- `fact_price_quote` — main price quote returned by the PBM (transaction_id, patient_plan_type, plan_id, drug_ndc, pharmacy_type, price_quote, deductible_remaining, OOP_remaining, PA/step-therapy flags, response_timestamp)
- `fact_covered_alternative` — alternative drugs returned along with the quote (transaction_id, alt_drug_ndc, alt_drug_name, alt_drug_type = generic / preferred_brand / therapeutic, alt_price_quote, pharmacy_type)
- `fact_claim` — downstream claim view for the dispensed prescription (claim_id, transaction_id, plan_id, drug_ndc, paid_amount, patient_pay_amount, reversal flags)

**Supporting / rules / audit**

- `formulary_rule` — synthetic formulary logic driving coverage & alternatives (plan_id, drug_ndc, tier, PA required, step therapy, quantity limits)
- `pa_request` — prior authorization requests (pa_id, member_id, drug_ndc, plan_id, status, decision timestamps)
- `audit_event` — audit trail of RTPB calls and responses (transaction_id, event_type, who queried, when, system, result_code)

### How Silver is built

- **Bronze** tables are 1:1 copies of the raw CSVs (no business logic, just typed storage).
- **Silver** tables enrich facts with dimensional context:
  - `price_silver` joins `fact_price_quote` to `dim_member` (in this synthetic dataset, the natural key is `plan_id`, so enrichment is at **plan grain**).
  - `alt_silver` currently persists `fact_covered_alternative` as-is; in a full Databricks version you can join via `transaction_id` back to `price_silver` to inherit member/plan attributes.
  - `claims_silver` behaves similarly, joining to member/plan where keys are available.

The local pandas scripts intentionally **auto-detect join keys** (member vs plan) and log what they use, which mirrors real-world messy healthcare data where grains are not always perfectly aligned.

### Gold KPIs

Gold tables are built from Silver:

- `avg_oop_by_plan` – average out-of-pocket cost by `plan_id` and `patient_plan_type` (Commercial vs DoD).
- `dod_vs_commercial_oop` – simple comparison of average OOP between DoD and Commercial segments.
- `generic_switch_rate_by_plan` – where possible, computes the rate at which a cheaper **generic alternative** is available and cheaper than the original price quote (using `transaction_id`, `price_quote`, `alt_price_quote`, and `alt_drug_type`).

These reflect typical questions PBMs, payers, and actuaries ask of RTPB data:
- “How much are members actually paying, by plan and segment?”
- “How much cheaper are DoD plans vs Commercial?”
- “What percentage of opportunities result in a generic switch?”

### HIPAA / PHI & synthetic data

All data in this repo is **fully synthetic** and designed to be **HIPAA-safe**:

- No names, addresses, phone numbers, emails, SSNs, MRNs, or any of the 18 HIPAA PHI identifiers are present in an identifiable way.
- Keys like `member_id`, `plan_id`, `transaction_id`, and `claim_id` are **random, artificial identifiers** with no mapping to real people or real claims.
- Dates are relative and synthetic, not tied to actual events or individuals.

This lets you talk about RTPB, PBMs, PHI handling, and HIPAA in interviews **without exposing any real patient data**.
