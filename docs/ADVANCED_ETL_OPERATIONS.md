# Advanced ETL Operations Integration

## Overview

This document describes the advanced ETL operations integrated into the operational dashboard, providing enhanced capabilities for data management, validation, and maintenance.

## Features Implemented

### 1. Materialized View Management

**Location**: Operational Dashboard → MV Management Section

**Features**:
- **MV Difference Scanning**: Compare materialized views with source parquet data to identify discrepancies
- **Simple MV Refresh**: Refresh all materialized views with basic options
- **Cascading MV Refresh**: Advanced refresh with auto-fetch and dependency tracking

**UI Components**:
- Scan MV Differences button (filled variant)
- Refresh All MVs button (light variant)  
- Cascading MV Refresh button (light, green variant)
- MV scan status display
- Results table showing MV name, status, missing days, and date counts

### 2. Aggregate Management

**Location**: Operational Dashboard → Aggregate Management Section

**Features**:
- **Sales Aggregates**: Build daily sales aggregates for performance optimization
- **Profit Aggregates**: Build daily profit aggregates for reporting
- **All Aggregates**: Build both sales and profit aggregates

**UI Components**:
- Build Sales Aggregates button (light variant)
- Build Profit Aggregates button (light variant)
- Build All Aggregates button (light, blue variant)
- Aggregate build status display

### 3. Data Validation

**Location**: Operational Dashboard → Data Validation Section

**Features**:
- **Profit Validation**: Comprehensive validation of profit data integrity
- **Detailed Results**: Shows validation status, record counts, and issues per day
- **Progress Indicators**: Visual progress bars and success rate percentages

**UI Components**:
- Validate Profit Data button (light variant)
- Validation status display with color-coded alerts
- Summary statistics with valid/invalid day counts and success rate
- Detailed results table with date, status, records, and issues

### 4. Enhanced Bulk Repair

**Location**: Operational Dashboard → Bulk Scan + Repair Modal

**Advanced Options** (collapsible accordion):
- **Auto-fetch Missing Data**: Automatically fetch missing POS/invoice data before refresh
- **Refresh Dimensions**: Update dimension tables (products, partners, etc.)
- **Build Aggregates**: Rebuild aggregate tables after data refresh
- **Validate Profit**: Run profit validation after data changes
- **Refresh MVs**: Update materialized views after data changes

**Enhanced Workflow**:
1. Standard data refresh for missing/empty partitions
2. Optional advanced operations based on user selection
3. Automatic MV refresh for profit-affecting datasets
4. Progress tracking and status updates throughout

## Technical Implementation

### Backend Integration

**ETL Data Manager Classes**:
- `MVScanner`: Materialized view difference scanning and management
- `DataScanner`: Data partition scanning and validation
- `BackfillRunner`: Aggregate building and profit validation

**Key Functions**:
```python
# MV Management
scanner.scan_mv_differences(start_date, end_date)
runner.refresh_materialized_views_cascading(views, start_date, end_date, auto_fetch, refresh_dims)

# Aggregate Building  
runner.backfill_aggregates('sales_aggregates', start_date, end_date)
runner.backfill_aggregates('profit_aggregates', start_date, end_date)

# Profit Validation
runner.validate_profit(start_date, end_date)
```

### Error Handling & Logging

**Comprehensive Error Handling**:
- Try-catch blocks around all ETL operations
- Detailed error messages with context
- Logging integration for debugging and monitoring
- User-friendly error displays with visual indicators

**Logging Configuration**:
```python
import logging
logger = logging.getLogger(__name__)

# Usage
logger.info(f"Starting MV scan for {start_date} to {end_date}")
logger.error(f"MV scan failed: {str(e)}", exc_info=True)
```

### Progress Indicators

**Visual Feedback**:
- Progress bars for long-running operations
- Color-coded status badges (green/orange/red)
- Percentage completion indicators
- Success rate calculations for validation results

**Status Updates**:
- Real-time status messages with emojis for quick recognition
- Detailed progress information in bulk operations
- Context-aware error messages with next steps

### Export Functionality

**CSV Export**:
- MV scan results export
- Aggregate build results export  
- Validation results export
- Bulk operation job details export

**Export Implementation**:
```python
@dash.callback(
    dash.Output('etl-ops-mv-results', 'exportDataAsCsv'),
    dash.Input('etl-ops-mv-export', 'n_clicks'),
    prevent_initial_call=True,
)
def export_etl_ops_mv(n_clicks):
    return True
```

## Usage Examples

### MV Difference Scanning
1. Navigate to Operational Dashboard
2. Select date range (default: last 7 days)
3. Click "Scan MV Differences" 
4. Review results table for synchronization status
5. Export results if needed

### Bulk Repair with Advanced Options
1. Navigate to Operational Dashboard
2. Select date range and datasets
3. Click "Bulk Scan + Repair"
4. In modal, expand "Advanced Options" accordion
5. Enable desired options:
   - Auto-fetch Missing Data: For POS/invoice data gaps
   - Refresh Dimensions: For inventory-related datasets
   - Build Aggregates: After data refresh
   - Validate Profit: For profit data integrity
   - Refresh MVs: For dashboard performance
6. Click "Run Bulk Repair" to start
7. Monitor progress in real-time
8. Export job details when complete

### Profit Validation
1. Navigate to Operational Dashboard
2. Select date range covering profit data
3. Click "Validate Profit Data"
4. Review validation summary with success rate
5. Examine detailed results for specific issues
6. Export validation results for analysis

## Benefits

### Performance Improvements
- **Reduced Query Times**: Pre-aggregated data for 30-day queries
- **Better Cache Hit Rates**: Optimized aggregate tables
- **Faster Dashboard Loads**: Materialized view refresh automation

### Data Quality
- **Proactive Issue Detection**: MV difference scanning identifies sync problems
- **Comprehensive Validation**: Multi-level profit data integrity checks
- **Automated Corrections**: Auto-fetch and refresh capabilities

### Operational Efficiency  
- **One-Click Operations**: Bulk repair with multiple advanced options
- **Progress Tracking**: Real-time feedback on long-running operations
- **Export Capabilities**: Easy data export for analysis and reporting

## Troubleshooting

### Common Issues

**MV Scan Fails**:
- Check Redis connection for MV metadata
- Verify parquet file permissions and paths
- Review DuckDB view definitions

**Aggregate Building Errors**:
- Ensure sufficient raw data exists for date range
- Check disk space for new aggregate files
- Validate ETL pipeline dependencies

**Profit Validation Issues**:
- Verify profit calculation logic in ETL pipelines
- Check for missing cost events or sales data
- Review validation rules and thresholds

**Bulk Repair Timeouts**:
- Reduce date range for large operations
- Check Celery worker status and capacity
- Monitor Redis memory usage

### Performance Monitoring

**Key Metrics**:
- MV refresh duration and success rates
- Aggregate build times and file sizes
- Validation error rates and patterns
- Bulk operation completion times

**Monitoring Tools**:
- Application logs for detailed error tracking
- Redis metrics for task queue health
- Dashboard performance analytics

## Future Enhancements

### Potential Improvements
- **Scheduled Operations**: Automated periodic MV refresh and validation
- **Alerting System**: Notifications for validation failures
- **Advanced Analytics**: Historical operation success rates and patterns
- **API Endpoints**: Programmatic access to ETL operations

### Integration Opportunities
- **Data Lineage**: Track data flow through ETL pipelines
- **Quality Metrics**: Automated quality scoring and trending
- **Performance Baselines**: Establish and monitor performance benchmarks

## Support

### Documentation Updates
- Update this document for new features and changes
- Maintain API reference for ETL operations
- Create troubleshooting guides for common issues

### Training Resources
- User guide for advanced ETL operations
- Video tutorials for bulk repair workflows
- Best practices documentation for data validation
