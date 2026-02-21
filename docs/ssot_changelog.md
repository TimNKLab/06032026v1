# NKDash SSOT Changelog

This document tracks changes to the SSOT (Single Source of Truth) documentation structure and content.

## Version 3.0 - 2026-02-21

### Major Refactoring
**Breaking Change**: Complete restructuring of SSOT.md from monolithic 2,000+ line document to streamlined 500-word pointer document.

### Changes Made

#### New Document Structure
- **SSOT.md** - Streamlined coordination document (500 words max)
- **docs/runbook.md** - Operational procedures and troubleshooting
- **docs/decisions.md** - Append-only decision log
- **docs/inventory_spec.md** - Inventory dashboard specification
- **docs/performance_policy.md** - Chart building and query optimization policies
- **docs/ssot_changelog.md** - This changelog document

#### Content Improvements
- Removed capability declaration section (legal disclaimer)
- Fixed placeholder ownership sections (TEAM-001 → [NAME - TBD])
- Added completion dates to all milestones
- Elevated critical blockers to top with 🚨 tagging
- Added document versioning and next review date
- Extracted detailed technical content to specialized docs

#### Issues Addressed
- **Monolithic document**: Split into focused, maintainable documents
- **Information architecture**: Clear separation of concerns
- **Version control**: Added changelog and version tracking
- **Ownership gaps**: Made placeholder status explicit
- **Critical dependencies**: Highlighted stock.quant requirement

### Files Modified
- `SSOT.md` - Complete rewrite (streamlined)
- `docs/runbook.md` - New file (operational procedures)
- `docs/decisions.md` - New file (decision log)
- `docs/inventory_spec.md` - New file (inventory specification)
- `docs/performance_policy.md` - New file (performance policies)
- `docs/ssot_changelog.md` - New file (this changelog)

### Impact Assessment
- **Maintainability**: Significantly improved through focused documents
- **Discoverability**: Better through clear document purposes
- **Onboarding**: Easier with structured documentation
- **Compliance**: Better audit trail through changelog

### Migration Notes
- All content from original SSOT.md preserved in specialized docs
- Links and references updated throughout codebase
- No breaking changes to technical implementation
- Decision log preserved with full historical context

---

## Version 2.x - Historical

### Version 2.3 - 2026-02-21
- Added profit ETL performance optimization documentation
- Updated decision log with beginning costs implementation
- Added workstream NK_20260221_beginning_costs_1f3c

### Version 2.2 - 2026-02-10  
- Added sales transaction filtering fix documentation
- Updated decision log with Feb 10 discrepancy resolution
- Added profit ETL validation evidence

### Version 2.1 - 2026-02-06
- Added profit ETL performance optimization section
- Updated decision log with performance improvements
- Added monitoring script documentation

### Version 2.0 - 2026-02-01
- Added comprehensive profit ETL documentation
- Updated milestone tracking with M6 completion
- Added inventory KPI implementation details

### Version 1.5 - 2026-01-21
- Added inventory adjustments workstream documentation
- Updated decision log with ABC analysis changes
- Added stock quant snapshot requirements

### Version 1.4 - 2026-01-19
- Added inventory KPI specification (Section 9)
- Updated milestone plan with M6
- Added workstream NK_20260119_inventory_kpis_3f2a

### Version 1.3 - 2026-01-15
- Added Odoo data sources documentation (Section 11)
- Updated decision log with data flow information
- Added derived tables documentation

### Version 1.2 - 2026-01-10
- Added validation standard (Option C)
- Updated oversight team section
- Added progress-vs-plan checklist

### Version 1.1 - 2025-12-20
- Added ETL reliability documentation
- Updated milestone tracking
- Added docker-compose usage instructions

### Version 1.0 - 2025-12-05
- Initial SSOT creation
- Basic milestone tracking
- Oversight team definition

---

## Document Standards

### Version Numbering
- **Major versions** (X.0): Structural changes, new document creation
- **Minor versions** (X.Y): Content additions, milestone updates
- **Patch versions** (X.Y.Z): Typo fixes, link updates

### Change Categories
- **Breaking Change**: Structural changes affecting document usage
- **Major Feature**: New sections or significant content additions  
- **Minor Feature**: Content updates, milestone progress
- **Documentation**: Typos, formatting, link fixes

### Review Schedule
- **Next Review**: 2026-03-07 (bi-weekly during active development)
- **Major Review**: Quarterly or after significant project changes
- **Urgent Review**: As needed for critical issues or blockers

---

## Related Documentation

### SSOT Linked Documents
- `docs/ARCHITECTURE.md` - Data lake and ETL architecture
- `DOCUMENTATION.md` - Technical documentation and ETL catalog
- `RELIABILITY.md` - ETL reliability and monitoring notes

### Project Documentation
- `docs/PROFIT_ETL_IMPLEMENTATION.md` - Profit ETL implementation guide
- `README.md` - Project overview and getting started

### External References
- Original critique that prompted refactoring (see decision log)

---

## Governance

### Approval Process
- **Structural changes**: Require project owner approval
- **Content updates**: Can be made by any team member
- **Version bumps**: Should be documented in changelog

### Quality Standards
- All documents must follow markdown formatting standards
- Links should be verified during updates
- Technical content should be validated against codebase

### Access Control
- All team members can propose changes
- Core team members can approve and merge
- Historical versions preserved in git history

---

*This changelog should be updated for every version change to the SSOT documentation.*  
*For detailed decision history, see docs/decisions.md*  
*Last updated: 2026-02-21*
