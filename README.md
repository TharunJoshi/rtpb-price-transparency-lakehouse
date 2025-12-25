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
