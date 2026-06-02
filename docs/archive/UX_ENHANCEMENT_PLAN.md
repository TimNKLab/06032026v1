# Dashboard UX Enhancement Plan
**Workstream:** NK_20260408_ux_responsiveness_a1b2
**Status:** Planning (Revised)
**Created:** 2026-04-08
**Revised:** 2026-04-08

## Problem Statement

The dashboard currently has poor UX responsiveness:
1. Only shows "Updating..." in the browser title during data fetches - not informative
2. No visual indication of what operation is running or progress
3. Abrupt running operations continue if user navigates away
4. Some pages auto-load data on visit without user consent

## Current State Analysis

### Loading Patterns Across Pages

| Page | Auto-Load on Visit | Loading Indicator | Trigger Pattern |
|------|-------------------|-------------------|-----------------|
| home.py | Yes (prevent_initial_call=False) | None (title only) | Apply button + preset buttons |
| sales.py | Yes (prevent_initial_call=False) | dcc.Loading (dot) on individual charts | Apply button + preset buttons |
| sales_drilldown.py | Yes (prevent_initial_call=False) | None | Auto from global context |
| inventory.py | Partial (exec summary auto-loads, tabs require click) | dcc.Loading (dot) on charts/tables | Refresh button per tab |
| customer.py | N/A (placeholder) | N/A | N/A |
| operational.py | No (prevent_initial_call=True) | dmc.Modal + LoadingOverlay with progress | Explicit trigger buttons |

### Key Findings

1. **operational.py has the best pattern**: Uses `dmc.Modal` with `dmc.LoadingOverlay` for async operations with:
   - Progress bar
   - Status text
   - Job table showing task states
   - Explicit open/close control
   - **Two-callback architecture**: trigger callback dispatches Celery task, polling callback updates progress

2. **Inconsistent auto-loading behavior**:
   - home.py: Auto-loads on first visit
   - sales.py: Auto-loads charts on visit
   - inventory.py: Executive summary auto-loads, tabs require explicit click
   - operational.py: Never auto-loads, always explicit

3. **No navigation cancellation**: Running operations continue even if user navigates away

## Solution Design

### Architecture Decision: Two-Callback Pattern (Not Generators)

**Critical clarification**: Standard Dash callbacks are request/response - they fire once and return once. The `yield` pattern does NOT work in standard callbacks. We must use one of these approaches:

| Approach | Use Case | Pros | Cons |
|----------|----------|------|------|
| **Two-callback + dcc.Interval** | Simple DB queries | Simple, no Celery dependency | No true progress, just spinner |
| **dash.long_callback** | Medium queries with progress | Built-in progress support | Requires background callback manager |
| **Celery + polling** (operational.py pattern) | Long-running ETL operations | True async, cancellable | Requires Celery infrastructure |

**Decision**: Use **two-callback + dcc.Interval** for most pages (home, sales, inventory, sales_drilldown). These pages query DuckDB directly and complete in <5s. Reserve Celery+polling for operational.py which already has it.

### 1. Shared Loading Modal Component

Create a reusable loading modal component in `components/loading_modal.py`:

```python
import dash_mantine_components as dmc
from dash import html

def create_loading_modal(
    modal_id: str,
    status_id: str,
    error_id: str,
    cancel_id: str,
    title: str = "Loading Data",
    show_cancel: bool = True,
    show_progress: bool = True,
):
    """
    Reusable loading modal with status, progress, error state, and cancel button.
    
    The modal has three states:
    1. Loading: Shows spinner + status text
    2. Error: Shows error message + retry button
    3. Complete: Modal closes automatically
    
    Args:
        modal_id: Unique ID for the modal component
        status_id: ID for status text element
        error_id: ID for error message element (hidden when no error)
        cancel_id: ID for cancel button
        title: Modal title text
        show_cancel: Whether to show cancel button (for long operations)
        show_progress: Whether to show progress bar (vs indeterminate spinner)
    """
    content = [
        dmc.Text("Initializing...", id=status_id, size="sm", c="dimmed", ta="center"),
    ]
    
    if show_progress:
        # Use indeterminate progress for operations without measurable progress
        # Animated striped bar at 0% looks like indeterminate
        content.append(
            dmc.Progress(
                id=f"{modal_id}-progress",
                value=0,
                striped=True,
                animated=True,
                mt="sm",
            )
        )
    else:
        # Simple spinner for quick operations
        content.append(
            dmc.Center(
                dmc.Loader(variant="dots", size="lg"),
                mt="lg",
            )
        )
    
    # Error message (hidden by default)
    content.append(
        dmc.Box(
            id=error_id,
            style={"display": "none"},
            children=[
                dmc.Alert(
                    "An error occurred while loading data.",
                    title="Error",
                    color="red",
                    variant="filled",
                    mt="sm",
                ),
                dmc.Button(
                    "Retry",
                    id=f"{modal_id}-retry",
                    variant="light",
                    color="blue",
                    mt="sm",
                    fullWidth=True,
                ),
            ],
        )
    )
    
    # Cancel button for long-running operations
    if show_cancel:
        content.append(
            dmc.Button(
                "Cancel",
                id=cancel_id,
                variant="subtle",
                color="gray",
                mt="md",
                fullWidth=True,
            )
        )
    
    return dmc.Modal(
        id=modal_id,
        opened=False,
        title=title,
        size="md",
        centered=True,
        withCloseButton=False,  # Prevent accidental close during operation
        closeOnClickOutside=False,  # Prevent accidental close
        children=[
            dmc.Box(
                content,
                p="md",
            )
        ],
    )


def create_simple_spinner_overlay(spinner_id: str, container_id: str):
    """
    Simple overlay spinner for quick operations (<2s).
    Use this instead of full modal for simple chart updates.
    
    This wraps content in a relative Box with LoadingOverlay.
    The LoadingOverlay must have children to overlay over.
    """
    return dmc.Box(
        id=container_id,
        pos="relative",
        children=[
            dmc.LoadingOverlay(
                id=spinner_id,
                visible=False,
                overlayProps={"radius": "sm", "blur": 2},
                zIndex=1000,
            ),
            # Content to overlay goes here as children
        ],
    )
```

### 2. Two-Callback Pattern for Simple Queries

For pages that query DuckDB directly (home, sales, inventory, sales_drilldown), use this pattern:

**Callback 1: Trigger + Execute**
```python
@dash.callback(
    Output('chart-id', 'figure'),
    Output('loading-modal', 'opened'),
    Output('loading-status', 'children'),
    Output('loading-error', 'style'),
    Input('apply-button', 'n_clicks'),
    State('date-from', 'value'),
    State('date-until', 'value'),
    prevent_initial_call=True,
)
def trigger_data_load(n_clicks, date_from, date_until):
    """Execute query and return results. Modal opens at start, closes at end."""
    if n_clicks is None:
        raise PreventUpdate
    
    ctx = dash.callback_context
    if not ctx.triggered:
        raise PreventUpdate
    
    # Open modal, show loading state
    try:
        # Query data (this is synchronous - user sees spinner)
        data = query_data(date_from, date_until)
        
        # Build figure
        figure = build_figure(data)
        
        # Success: close modal, clear error
        return (
            figure,
            False,  # Close modal
            "Complete",
            {"display": "none"},  # Hide error
        )
    except Exception as e:
        # Error: keep modal open, show error
        return (
            dash.no_update,  # Keep existing figure
            True,  # Keep modal open
            f"Error: {str(e)}",
            {"display": "block"},  # Show error
        )
```

**Key points:**
- Modal opens at callback start (Output `opened=True`)
- Modal closes at callback end (Output `opened=False`)
- User sees spinner while callback executes
- No `yield` needed - just return final state
- Error handling keeps modal open with error message

### 3. Three-Callback Pattern for Long Operations (Celery)

For pages that use Celery (operational.py already has this), extend the existing pattern:

**Callback 1: Trigger + Dispatch Celery Task**
```python
@dash.callback(
    Output('loading-modal', 'opened'),
    Output('loading-status', 'children'),
    Output('active-task-id-store', 'data'),
    Output('loading-error', 'style'),
    Input('apply-button', 'n_clicks'),
    State('date-from', 'value'),
    State('date-until', 'value'),
    prevent_initial_call=True,
)
def dispatch_celery_task(n_clicks, date_from, date_until):
    """Dispatch Celery task and open modal."""
    if n_clicks is None:
        raise PreventUpdate
    
    try:
        # Dispatch Celery task
        result = my_celery_task.delay(date_from, date_until)
        
        # Open modal, show task ID
        return (
            True,  # Open modal
            f"Task dispatched: {result.id}",
            {"task_id": result.id, "status": "PENDING"},
            {"display": "none"},
        )
    except Exception as e:
        return (
            True,
            f"Error dispatching task: {str(e)}",
            {},
            {"display": "block"},
        )
```

**Callback 2: Poll Task Status**
```python
@dash.callback(
    Output('loading-status', 'children', allow_duplicate=True),
    Output('loading-modal-progress', 'value'),
    Output('loading-modal', 'opened', allow_duplicate=True),
    Output('loading-error', 'style', allow_duplicate=True),
    Input('polling-interval', 'n_intervals'),
    State('active-task-id-store', 'data'),
    prevent_initial_call=True,
)
def poll_task_status(n_intervals, task_data):
    """Poll Celery task status and update modal."""
    if not task_data or not task_data.get('task_id'):
        raise PreventUpdate
    
    from celery.result import AsyncResult
    result = AsyncResult(task_data['task_id'])
    
    if result.state == 'PENDING':
        return "Waiting for task to start...", 0, True, {"display": "none"}
    elif result.state == 'PROGRESS':
        progress = result.info.get('progress', 0)
        status = result.info.get('status', 'Processing...')
        return status, progress, True, {"display": "none"}
    elif result.state == 'SUCCESS':
        # Task complete - close modal
        return "Complete!", 100, False, {"display": "none"}
    elif result.state == 'FAILURE':
        error_msg = str(result.info)
        return f"Error: {error_msg}", 0, True, {"display": "block"}
    
    raise PreventUpdate
```

**Callback 3: Cancel Task**
```python
@dash.callback(
    Output('loading-modal', 'opened', allow_duplicate=True),
    Output('active-task-id-store', 'data', allow_duplicate=True),
    Output('loading-status', 'children', allow_duplicate=True),
    Input('cancel-button', 'n_clicks'),
    State('active-task-id-store', 'data'),
    prevent_initial_call=True,
)
def cancel_task(n_clicks, task_data):
    """Cancel running Celery task."""
    if n_clicks is None or not task_data:
        raise PreventUpdate
    
    from celery.result import AsyncResult
    task_id = task_data.get('task_id')
    if task_id:
        AsyncResult(task_id).revoke(terminate=True)
    
    return False, {}, "Task cancelled"
```

### 4. Navigation Cancellation (Celery Only)

**Critical limitation**: Standard Dash callbacks cannot be cancelled mid-flight. Navigation cancellation only works for:
- Celery tasks (via `AsyncResult.revoke()`)
- Background callbacks (via `long_callback` cancellation)

For synchronous DB queries, the query completes regardless of navigation. The modal simply won't be visible on the new page.

```python
# In app.py - global navigation handler
@app.callback(
    Output('global-task-store', 'data', allow_duplicate=True),
    Input('app-location', 'pathname'),
    State('global-task-store', 'data'),
    prevent_initial_call=True,  # Don't fire on initial load
)
def handle_navigation_cancel(pathname, task_data):
    """
    Cancel active Celery task when user navigates away.
    Only works for Celery tasks, NOT for synchronous callbacks.
    """
    if task_data and task_data.get('task_id'):
        from celery.result import AsyncResult
        AsyncResult(task_data['task_id']).revoke(terminate=True)
        return {'task_id': None, 'cancelled': True}
    return dash.no_update
```

### 5. Modal Contention Resolution

For pages with multiple independent operations (inventory.py has 4 tabs), use **operation-scoped modals**:

```python
# Each tab gets its own modal
layout = dmc.Container([
    # Tab 1 modal
    create_loading_modal(
        modal_id='exec-loading-modal',
        status_id='exec-loading-status',
        error_id='exec-loading-error',
        cancel_id='exec-cancel',
        title="Loading Executive Summary",
        show_cancel=False,  # Quick operation
    ),
    
    # Tab 2 modal
    create_loading_modal(
        modal_id='stock-loading-modal',
        status_id='stock-loading-status',
        error_id='stock-loading-error',
        cancel_id='stock-cancel',
        title="Loading Stock Levels",
        show_cancel=False,
    ),
    
    # ... etc for each tab
])
```

**Alternative**: Use single modal with operation context:

```python
# Single modal with context store
dcc.Store(id='active-operation', data=None)  # 'exec', 'stock', 'sell-through', 'abc'

# Each callback outputs to same modal but with different status
@dash.callback(
    Output('chart-id', 'figure'),
    Output('loading-modal', 'opened'),
    Output('loading-status', 'children'),
    Output('active-operation', 'data'),
    Input('apply-button', 'n_clicks'),
    prevent_initial_call=True,
)
def load_exec_summary(n_clicks):
    # ... query data ...
    return figure, False, "Executive summary loaded", None
```

**Decision**: Use operation-scoped modals for inventory.py (4 separate modals, one per tab). This avoids callback conflicts and is clearer for users.

### 6. Specific Page Changes

#### home.py
- Change `prevent_initial_call=False` to `True` for main callback
- Add loading modal to layout
- Show "Click Apply to load data" placeholder initially
- Use two-callback pattern (simple query)

#### sales.py
- Change all `prevent_initial_call=False` to `True`
- Add loading modal to layout
- Use two-callback pattern (simple query)
- Show "Click Apply to load data" placeholder initially

#### inventory.py
- Executive summary already has explicit trigger (Refresh button)
- Replace individual `dcc.Loading` with operation-scoped modals (one per tab)
- Use two-callback pattern for each tab

#### sales_drilldown.py
- Change `prevent_initial_call=False` to `True`
- Add explicit "Load Data" button
- Add loading modal
- Use two-callback pattern

#### customer.py
- No changes needed (placeholder page)

#### operational.py
- Already has correct Celery+polling pattern
- Add cancel button to modal
- Add error state handling
- No other changes needed

## Implementation Steps

### Phase 1: Infrastructure (High Priority)
1. Create `components/loading_modal.py` with reusable component
2. Add global task store to `app.py` (for Celery cancellation)
3. Test component in isolation

### Phase 2: Page Updates (High Priority)
4. Update home.py with two-callback pattern + modal
5. Update sales.py with two-callback pattern + modal
6. Update inventory.py with operation-scoped modals
7. Update sales_drilldown.py with two-callback pattern + modal

### Phase 3: Navigation Handling (Medium Priority)
8. Add navigation cancellation callback to `app.py` (Celery only)
9. Document limitation: synchronous queries cannot be cancelled
10. Add visual indicator when operation was cancelled (for Celery tasks)

### Phase 4: Testing & Polish (Medium Priority)
11. Test all pages for correct loading behavior
12. Test error handling and retry functionality
13. Test cancel button (for Celery operations)
14. Add timeout handling (show error if modal open > 30s)
15. Polish modal styling to match DESIGN.md guidelines

### Phase 5: Documentation (Medium Priority)
16. Update SSOT.md with M7 completion
17. Document new loading pattern in DOCUMENTATION.md
18. Update runbook with troubleshooting for loading issues

## Technical Considerations

### Performance
- Loading modal adds minimal overhead (DMC component)
- Avoids unnecessary database queries on page visit
- Reduces initial page load time

### Backward Compatibility
- Changes are additive (no breaking changes to data layer)
- Existing API endpoints unaffected
- Celery tasks unchanged

### Limitations
1. **Synchronous callbacks cannot be cancelled**: Once a DuckDB query starts, it completes regardless of navigation
2. **No true progress for simple queries**: Progress bar is indeterminate (animated at 0%) for synchronous operations
3. **Modal contention**: Multiple operations on same page require separate modals or careful state management

### Edge Cases
1. **Multiple tabs open**: Each tab has its own loading state (memory storage is per-tab)
2. **Slow queries**: Add timeout with error message after 30s
3. **Network errors**: Show error in modal with retry button
4. **Celery task cancellation**: Use `AsyncResult.revoke(terminate=True)` for async operations

### Design Alignment
Follow DESIGN.md guidelines:
- Use Cohere color palette (Interaction Blue #1863dc for loading states)
- 22px border radius for modal
- Unica77 font for body text
- Minimal shadows, rely on borders
- Red for errors, Gray for cancel buttons

## Success Criteria

1. [ ] No page auto-loads data without explicit user action
2. [ ] Loading modal shows with clear status message
3. [ ] Error handling displays error message with retry option
4. [ ] Cancel button works for Celery operations
5. [ ] Navigation away from page cancels Celery tasks (not synchronous queries)
6. [ ] Consistent pattern across all pages
7. [ ] Performance: Initial page load < 2s
8. [ ] All existing functionality preserved

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| User confusion about explicit load buttons | Medium | Add clear "Click Apply to load data" placeholder |
| Synchronous queries cannot be cancelled | Low | Document limitation; most queries complete in <5s |
| Modal contention on multi-operation pages | Medium | Use operation-scoped modals (one per tab) |
| Increased callback complexity | Medium | Create helper functions for common patterns |
| Error state not handled | Medium | Add error output to modal with retry button |

## Open Questions - RESOLVED

1. **Should loading state persist across page navigation?** 
   - Decision: No, reset on navigation. Memory storage is per-tab already.

2. **Should we add a "Cancel" button to the loading modal?**
   - Decision: Yes, for Celery operations. Hide for simple synchronous queries.

3. **What timeout threshold for showing error message?**
   - Decision: 30s. Show error if modal open > 30s.

4. **How to handle progress for simple queries?**
   - Decision: Use indeterminate progress bar (animated striped at 0%). Don't show arbitrary percentages.

## Related Workstreams

- NK_20260126_design_enhancement_4a7c: UI/UX enhancement (DMC framework) - parent workstream
- NK_20260206_profit_etl_9a2b: Profit ETL implementation - may need loading updates

## Next Actions

1. Create `components/loading_modal.py` with corrected component
2. Update home.py as proof of concept with two-callback pattern
3. Test error handling and retry functionality
4. Iterate on remaining pages
