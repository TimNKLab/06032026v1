import re

with open('services/inventory_metrics.py', 'r') as f:
    content = f.read()

# Remove _query_sell_through function (lines 351-509)
# More specific pattern to avoid matching _query_abc_products
pattern1 = r'\ndef _query_sell_through\(snapshot_date: date, start_date: date, end_date: date\) -> pd\.DataFrame:.*?return result\n\n'
content = re.sub(pattern1, '# _query_sell_through removed - deferred due to complexity\n# Previously used SQLite MVs which no longer exist\n\n', content, flags=re.DOTALL)

# Remove get_sell_through_analysis function (lines 512-606)
pattern2 = r'\ndef get_sell_through_analysis\(start_date: date, end_date: date\) -> Dict\[str, object\]:.*?return \{[^}]+\}\n\n'
content = re.sub(pattern2, '# get_sell_through_analysis removed - depends on _query_sell_through\n\n', content, flags=re.DOTALL)

with open('services/inventory_metrics.py', 'w') as f:
    f.write(content)

print('Sell-through functions removed successfully')
