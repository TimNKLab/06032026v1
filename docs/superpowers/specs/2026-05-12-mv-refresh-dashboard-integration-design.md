# MV Refresh Dashboard Integration Design

**Date:** 2026-05-12  
**Author:** Cascade AI  
**Status:** Draft  
**Workstream:** NK_20260512_mv_refresh_dashboard_integration  

## Overview

Integrate Materialized View (MV) refresh functionality into the operational dashboard (`pages/operational.py`) to provide users with web-based access to MV refresh operations, replacing the current desktop GUI/CLI-only access.

## Requirements

### Functional Requirements
1. **Manual MV Refresh**: Users can trigger MV refresh from the operational dashboard
2. **All MVs**: Always refresh all available materialized views
3. **Date Range**: Use the same date range as ETL operations (user-selected)
4. **Background Processing**: Refresh runs in background with progress tracking
5. **Queue System**: Handle concurrent requests with proper queuing
6. **Priority**: ETL jobs always take priority over MV refresh
7. **Cron Integration**: Automatic MV refresh after daily ETL completion
8. **Status History**: Track and display MV refresh status and history

### Non-Functional Requirements
1. **Lock Handling**: Prevent DuckDB lock conflicts
2. **User Access**: All users can trigger MV refresh
3. **Container-Only**: MV refresh only accessible within Docker containers
4. **Error Handling**: Clear error messages and recovery options
5. **Performance**: Non-blocking UI with responsive feedback

## Architecture

### Components

#### 1. UI Integration (Operational Dashboard)
- **Location**: Controls card in `pages/operational.py`
- **New Elements**:
  - Button: "Refresh MVs" 
  - Switch: "Auto-refresh after ETL" (for cron behavior)
- **Status Display**: Extend existing polling mechanism for MV operations

#### 2. MV Queue Service
- **Implementation**: Redis-based queue
- **Purpose**: Serialize MV refresh requests
- **Features**:
  - Queue position tracking
  - Request deduplication
  - Priority handling (ETL > MV)

#### 3. MV Refresh Task
- **Type**: Celery task (`refresh_materialized_views`)
- **Behavior**:
  - Check DuckDB lock status
  - Wait if ETL operations running
  - Execute CLI refresh via Docker delegation
  - Update status in Redis
  - Handle errors gracefully

#### 4. Cron Integration
- **Task**: `refresh_materialized_views_scheduled`
- **Trigger**: After daily ETL pipeline completion
- **Condition**: Only if ETL jobs succeeded
- **Scope**: All MVs for latest data

### Data Flow

```
User Clicks "Refresh MVs" 
    ↓
Queue MV Request (Redis)
    ↓
Background Celery Task
    ↓
Check DuckDB Lock Status
    ↓
Wait if ETL Running (with timeout)
    ↓
Execute CLI Refresh (Docker)
    ↓
Update Status & Notify
    ↓
Display Result in Dashboard
```

## Implementation Details

### Phase 1: Manual MV Refresh

#### UI Changes (`pages/operational.py`)
```python
# Add to Controls card button group
dmc.Button('Refresh MVs', id='etl-ops-mv-refresh', variant='light')

# Add to switches grid
dmc.Switch(
    id='etl-ops-auto-mv',
    label='Auto-refresh after ETL',
    description='Run MV refresh after ETL completion',
    size='sm',
)
```

#### New Celery Task (`etl_tasks.py`)
```python
@app.task(bind=True)
def refresh_materialized_views(self, start_date: str, end_date: str):
    """Refresh all materialized views for date range with proper locking."""
    # Implementation details in task section
```

#### Callback Handler
```python
@dash.callback(
    dash.Output('etl-ops-trigger-status', 'children'),
    dash.Input('etl-ops-mv-refresh', 'n_clicks'),
    [dash.State('etl-ops-date-start', 'value'),
     dash.State('etl-ops-date-end', 'value')],
    prevent_initial_call=True,
)
def trigger_mv_refresh(n_clicks, date_start, date_end):
    # Queue MV refresh and return status
```

### Phase 2: Queue System

#### Redis Queue Implementation
```python
# Queue structure
mv_queue = {
    'requests': [
        {'id': 'uuid', 'start': '2026-05-12', 'end': '2026-05-12', 
         'status': 'queued', 'position': 1, 'created': 'timestamp'}
    ],
    'current': None,  # Currently running request
    'lock_acquired': False  # DuckDB lock status
}
```

#### Lock Coordination
```python
def acquire_duckdb_lock(timeout=300):
    """Try to acquire DuckDB write lock with timeout."""
    
def release_duckdb_lock():
    """Release DuckDB write lock."""
    
def is_etl_running():
    """Check if any ETL tasks are currently running."""
```

### Phase 3: Cron Integration

#### Scheduled Task
```python
@app.task
def refresh_materialized_views_scheduled():
    """Scheduled MV refresh after ETL completion."""
    # Check if ETL jobs completed successfully
    # Trigger MV refresh for latest date range
    # Log results and update status
```

#### ETL Completion Hook
```python
# Add to existing ETL pipeline completion
@app.task
def daily_etl_pipeline_with_mv(date_str):
    """Daily ETL pipeline with automatic MV refresh."""
    # Run ETL pipeline
    # On success, trigger MV refresh if auto-refresh enabled
```

## Error Handling

### DuckDB Lock Conflicts
- **Detection**: Check for lock errors before refresh
- **Recovery**: Queue request and retry after delay
- **User Feedback**: "MV refresh queued (position X)"

### CLI Execution Failures
- **Detection**: Monitor CLI exit codes and output
- **Recovery**: Retry with exponential backoff
- **User Feedback**: Clear error messages with suggested actions

### Queue Timeouts
- **Detection**: Requests queued too long (> 30 minutes)
- **Recovery**: Cancel request and notify user
- **User Feedback**: "MV refresh timed out, please try again"

## Testing Strategy

### Unit Tests
- MV refresh task logic
- Lock acquisition/release
- Queue operations
- Error handling scenarios

### Integration Tests
- Dashboard UI interactions
- Celery task execution
- Redis queue behavior
- Docker CLI delegation

### Performance Tests
- Concurrent request handling
- Large date range refresh
- Lock contention scenarios

## Security Considerations

### Access Control
- All users can trigger MV refresh (current requirement)
- Future: Role-based access control

### Container Security
- MV refresh only accessible within Docker
- No external API endpoints
- Proper isolation from host system

## Monitoring & Logging

### Metrics
- MV refresh success/failure rates
- Average refresh duration
- Queue depth and wait times
- Lock contention frequency

### Logging
- Request queuing and completion
- Lock acquisition/release events
- Error details and recovery actions
- Performance metrics

## Deployment

### Environment Variables
```bash
# MV refresh configuration
MV_REFRESH_ENABLED=true
MV_AUTO_REFRESH_AFTER_ETL=false
MV_LOCK_TIMEOUT=300
MV_QUEUE_MAX_SIZE=10
```

### Docker Changes
- No new services required
- Use existing celery-worker for MV tasks
- Redis for queue storage (already available)

## Rollback Plan

### If Issues Occur
1. Disable MV refresh via environment variable
2. Clear Redis queue manually
3. Revert to CLI-only access
4. Monitor system stability

### Migration Path
- Gradual rollout with feature flag
- A/B testing with user groups
- Performance monitoring and optimization

## Success Criteria

1. **Functionality**: Users can refresh MVs from dashboard
2. **Reliability**: No DuckDB lock conflicts
3. **Performance**: Non-blocking UI with responsive feedback
4. **Usability**: Clear status messages and error handling
5. **Integration**: Seamless with existing ETL operations

## Future Enhancements

1. **Selective MV Refresh**: Allow users to choose specific MVs
2. **Advanced Scheduling**: Custom cron schedules for different MVs
3. **Performance Optimization**: Incremental MV refresh strategies
4. **Monitoring Dashboard**: Dedicated MV operations monitoring
5. **API Access**: REST endpoints for programmatic MV refresh

---

**Next Steps:**
1. Review and approve design
2. Create implementation plan
3. Begin Phase 1 development
4. Test and iterate
5. Deploy to production
