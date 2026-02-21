# NKDash Glossary

This document defines all acronyms, technical terms, and business concepts used throughout the NKDash system.

## Table of Contents
- [Acronyms](#acronyms)
- [Technical Terms](#technical-terms)
- [Business Terms](#business-terms)
- [Data Concepts](#data-concepts)
- [System Components](#system-components)

---

## Acronyms

| Acronym | Full Name | Context |
|---------|-----------|---------|
| **API** | Application Programming Interface | Odoo integration, system interfaces |
| **ABC** | Activity-Based Classification | Inventory analysis methodology |
| **CPU** | Central Processing Unit | System performance monitoring |
| **CSV** | Comma-Separated Values | Data export/import format |
| **DMS** | Document Management System | File organization |
| **ETL** | Extract, Transform, Load | Data processing pipeline |
| **GUI** | Graphical User Interface | Dashboard interface |
| **HTTP** | Hypertext Transfer Protocol | Web communications |
| **JSON** | JavaScript Object Notation | Data interchange format |
| **KPI** | Key Performance Indicator | Business metrics |
| **POS** | Point of Sale | Retail transaction system |
| **RAM** | Random Access Memory | System memory |
| **RPC** | Remote Procedure Call | Odoo communication protocol |
| **SLA** | Service Level Agreement | Performance targets |
| **SLO** | Service Level Objective | Performance goals |
| **SQL** | Structured Query Language | Database queries |
| **SSL** | Secure Sockets Layer | Security protocol |
| **TTL** | Time To Live | Cache expiration |
| **UI** | User Interface | Dashboard design |
| **UX** | User Experience | Usability design |
| **URL** | Uniform Resource Locator | Web addresses |

---

## Technical Terms

### Database & Storage
- **Parquet**: Columnar storage format optimized for analytics
- **DuckDB**: In-process analytical database optimized for OLAP workloads
- **Redis**: In-memory data structure store used for caching and message brokering
- **Schema**: Database structure definition (tables, columns, relationships)
- **Partition**: Data organization strategy for large datasets (usually by date)
- **Index**: Data structure that improves query performance
- **View**: Virtual table based on SQL query results

### Data Processing
- **Pipeline**: Sequence of data processing steps
- **Batch Processing**: Processing data in discrete groups
- **Stream Processing**: Processing data continuously as it arrives
- **Transformation**: Converting data from one format to another
- **Validation**: Ensuring data meets quality standards
- **Normalization**: Organizing data to reduce redundancy
- **Aggregation**: Combining multiple values into a single summary

### Software Architecture
- **Container**: Isolated application environment (Docker)
- **Microservices**: Architectural style with small, independent services
- **API Gateway**: Single entry point for multiple services
- **Load Balancer**: Distributes traffic across multiple servers
- **Middleware**: Software that connects applications
- **Framework**: Pre-built structure for application development

### Performance & Monitoring
- **Latency**: Time delay in data processing or response
- **Throughput**: Amount of data processed per time unit
- **Concurrency**: Handling multiple tasks simultaneously
- **Scalability**: Ability to handle increased load
- **Bottleneck**: Component that limits overall performance
- **Cache**: Temporary storage for frequently accessed data

---

## Business Terms

### Retail & Sales
- **Revenue**: Total income from sales before deductions
- **Gross Profit**: Revenue minus cost of goods sold
- **Margin**: Profit expressed as percentage of revenue
- **SKU**: Stock Keeping Unit - unique product identifier
- **Category**: Product grouping based on characteristics
- **Brand**: Product manufacturer or label
- **Transaction**: Single sales event
- **Receipt**: Record of purchase
- **Refund**: Return of money to customer
- **Discount**: Reduction in price

### Inventory Management
- **Stock**: Inventory of goods
- **Warehouse**: Storage facility for inventory
- **Supplier**: Entity that provides goods
- **Purchase Order**: Request to buy goods
- **Stock Level**: Current inventory quantity
- **Reorder Point**: Minimum stock level that triggers reorder
- **Lead Time**: Time between order and delivery
- **Carrying Cost**: Cost of holding inventory
- **Stockout**: Situation where inventory is depleted
- **Overstock**: Excess inventory

### Financial
- **Cost**: Amount paid for goods or services
- **Expense**: Business cost
- **Invoice**: Bill for goods/services
- **Account**: Record of financial transactions
- **Tax**: Government charge on transactions
- **Audit**: Systematic examination of records
- **Budget**: Financial plan
- **Forecast**: Prediction of future values

---

## Data Concepts

### Data Quality
- **Accuracy**: Degree to which data correctly represents reality
- **Completeness**: Presence of all required data
- **Consistency**: Uniformity of data across systems
- **Timeliness**: Currency of data relative to needs
- **Validity**: Conformance to defined rules
- **Uniqueness**: No duplicate records
- **Integrity**: Data remains unchanged during operations

### Data Types
- **Structured Data**: Organized data with clear schema (tables, spreadsheets)
- **Unstructured Data**: Data without predefined format (text, images)
- **Semi-structured Data**: Partially organized data (JSON, XML)
- **Time Series Data**: Data points indexed by time
- **Categorical Data**: Data that can be grouped into categories
- **Numerical Data**: Data in numeric form
- **Boolean Data**: True/false values

### Data Operations
- **Extract**: Pulling data from source systems
- **Transform**: Converting data format or structure
- **Load**: Inserting data into target system
- **Merge**: Combining datasets
- **Join**: Combining tables based on common keys
- **Filter**: Selecting subset of data based on criteria
- **Sort**: Arranging data in specific order
- **Group**: Organizing data by categories
- **Pivot**: Restructuring data from rows to columns

---

## System Components

### NKDash Components
- **Dash Application**: Web-based dashboard framework
- **Celery**: Distributed task queue for background processing
- **Redis**: Message broker and cache
- **Docker**: Container platform for application deployment
- **DuckDB**: Analytical database powering dashboards
- **Data Lake**: Centralized data repository
- **ETL Pipeline**: Data processing workflow
- **Star Schema**: Data warehouse modeling approach

### Odoo Integration
- **Odoo ERP**: Enterprise resource planning system
- **POS Module**: Point of sale functionality
- **Inventory Module**: Stock management
- **Accounting Module**: Financial management
- **CRM Module**: Customer relationship management
- **API Connector**: Interface for system integration

### Infrastructure
- **Container**: Isolated application environment
- **Volume**: Persistent storage for containers
- **Network**: Communication between containers
- **Environment Variables**: Configuration values
- **Logs**: System activity records
- **Backup**: Data copy for recovery
- **Monitoring**: System health tracking

---

## Domain-Specific Terms

### NKDash Specific
- **Fact Table**: Table containing business metrics (sales, inventory)
- **Dimension Table**: Table containing descriptive attributes (products, customers)
- **Data Lake**: Multi-layer storage system (raw, clean, star-schema)
- **KPI Dashboard**: Visual display of business metrics
- **ETL Task**: Specific data processing job
- **Pipeline**: Sequence of ETL tasks
- **View**: Virtual table for data access

### Business Metrics
- **Sell-through**: Ratio of units sold to units available
- **Days of Cover**: How long current inventory will last
- **ABC Analysis**: Product classification by revenue contribution
- **Pareto Curve**: Visual representation of 80/20 rule
- **Growth Rate**: Percentage change over time
- **Conversion Rate**: Percentage of actions completed
- **Customer Lifetime Value**: Total revenue from customer over time

### Technical Metrics
- **Query Performance**: Speed of data retrieval
- **ETL Runtime**: Time to process data
- **Data Freshness**: How current the data is
- **System Availability**: Percentage of time system is operational
- **Error Rate**: Frequency of errors
- **Response Time**: Time to respond to requests

---

## Related Concepts

### Data Warehousing
- **OLAP**: Online Analytical Processing
- **OLTP**: Online Transaction Processing
- **Data Mart**: Subset of data warehouse
- **Business Intelligence**: Tools for data analysis
- **Analytics**: Discovery of data insights

### Software Development
- **Version Control**: System for tracking code changes
- **CI/CD**: Continuous Integration/Continuous Deployment
- **Testing**: Process of verifying software quality
- **Documentation**: Information about system
- **Debugging**: Process of finding and fixing errors

### Operations
- **Deployment**: Process of releasing software
- **Monitoring**: Tracking system performance
- **Incident**: Unplanned interruption of service
- **Maintenance**: Activities to keep system running
- **Scaling**: Adjusting system capacity

---

## Usage Examples

### In Technical Context
- "The **ETL** **pipeline** extracts **POS** data from **Odoo** and loads it into **DuckDB**"
- "We need to optimize **query** **performance** for the **KPI** **dashboard**"
- "The **data** **lake** uses **Parquet** format with **partition** strategy"

### In Business Context
- "Our **sell-through** **rate** improved by 15% this quarter"
- "**ABC** **analysis** shows our top 20% **SKUs** generate 80% of **revenue**"
- "**Days** **of** **cover** for inventory is averaging 45 days"

### In Operational Context
- "The **Celery** **worker** is processing the **ETL** **task**"
- "**Redis** **cache** is improving **dashboard** response time"
- "We need to check the **logs** for the **failed** **pipeline**"

---

## Evolving Glossary

This glossary is a living document. New terms should be added as:
- New features are implemented
- Business requirements evolve
- Technical concepts are introduced
- User feedback indicates confusion

### Update Process
1. Identify new term or concept
2. Provide clear definition
3. Include context and usage examples
4. Cross-reference related terms
5. Update documentation references

---

*For technical implementation details, see ETL_GUIDE.md*  
*For business metrics definitions, see docs/inventory_spec.md*  
*For system architecture, see docs/ARCHITECTURE.md*  
*Last updated: 2026-02-21*
