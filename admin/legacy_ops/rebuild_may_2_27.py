from etl_tasks import daily_sales_aggregates_pipeline, daily_profit_pipeline

tasks = []
for day in range(2, 28):
    date_str = f'2026-05-{day:02d}'
    tasks.append(daily_sales_aggregates_pipeline.delay(date_str))
    tasks.append(daily_profit_pipeline.delay(date_str))
    print(f'Submitted tasks for {date_str}')

print(f'Total tasks submitted: {len(tasks)}')
