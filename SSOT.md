# NKDash — Single Source of Truth (SSOT)

## Document Version
- **Version:** 3.0
- **Last Validated:** 2026-02-21 (post-refactor)
- **Next Review:** 2026-03-07
- **Change Log:** See `docs/ssot_changelog.md`

## 🚨 Critical Blockers
1. **Stock.quant dependency** - Inventory KPIs require daily stock snapshots (marked optional but critical)
2. **Ownership gaps** - Oversight team roles use placeholders (TEAM-001, etc.)

## Purpose
Canonical coordination document for NKDash repository. Links to authoritative docs, tracks milestones, and provides project oversight.

## Current Phase
**Stabilization & Dataset Expansion** - Ensure reliable daily ETL pipelines while expanding dashboard capabilities.

## Quick Links (Authoritative Docs)
- **Architecture:** `docs/ARCHITECTURE.md` - Data lake + DuckDB architecture
- **Runbook:** `docs/runbook.md` - Operational procedures + troubleshooting  
- **Decisions:** `docs/decisions.md` - Append-only decision log
- **Inventory Spec:** `docs/inventory_spec.md` - Inventory KPIs + implementation plan
- **Performance Policy:** `docs/performance_policy.md` - Chart building + query optimization
- **Technical Docs:** `DOCUMENTATION.md`, `RELIABILITY.md`

## Milestone Status

| Milestone | Status | Completion Date |
|-----------|--------|-----------------|
| M0 — MVP dashboard | Validated | 2025-01-15 |
| M1 — Data lake + ETL decoupling | Validated | 2025-01-20 |
| M2 — Daily ETL pipelines | Validated | 2025-01-25 |
| M3 — Reliability (catch-up + health) | Validated | 2025-02-01 |
| M4 — Operational tooling | Validated | 2025-02-05 |
| M5 — Cloud + monitoring | Planned | TBD |
| M6 — Inventory KPIs | Done (code present) | 2025-02-10 |
| M7 — UI/UX enhancement | In Progress | 2026-02-21 |

## Validation Standard (Option C)
- **Correctness:** ≤0.5% revenue variance vs Odoo (3-date sampling)
- **Freshness:** ETL metadata within 1 day of today
- **Performance:** ETL < 30min, dashboard queries < 2s
- **Evidence:** All validations recorded in decision log

## Oversight Team
**Acting owners** (real names needed):
- **Project Owner:** [NAME - TBD]
- **Technical Lead:** [NAME - TBD] 
- **Data/ETL Owner:** [NAME - TBD]
- **Dashboard Owner:** [NAME - TBD]
- **Ops/Release Owner:** [NAME - TBD]

**Current process:** Weekly 30min sync, acting owner = person implementing changes

## Active Workstreams
- **NK_20260126_design_enhancement_4a7c** - UI/UX enhancement (DMC framework)
- **NK_20260121_adjustments_8d9b** - Inventory adjustments handling (in progress)

## Key Data Sources
- **Transactional:** pos.order, account.move (sales/purchases), stock.move.line, stock.quant
- **Dimensions:** product, category, brand, tax, partner
- **Derived:** Cost events, profit aggregates, inventory snapshots

## Next Steps
1. Assign real names to oversight roles
2. Make stock.quant snapshots mandatory for inventory KPIs
3. Complete M7 UI/UX enhancement
4. Plan M5 cloud deployment strategy
