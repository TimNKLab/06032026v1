import re

with open('services/inventory_metrics.py', 'r') as f:
    content = f.read()

# Find and remove _query_sell_through function
pattern1 = r'\ndef _query_sell_through\(snapshot_date: date, start_date: date, end_date: date\) -> pd\.DataFrame:.*?return result\n\n'
content = re.sub(pattern1, '', content, flags=re.DOTALL)

# Find and remove get_sell_through_analysis function
pattern2 = r'\ndef get_sell_through_analysis\(start_date: date, end_date: date\) -> Dict\[str, object\]:.*?return \{[^}]+\}\n\n'
content = re.sub(pattern2, '', content, flags=re.DOTALL)

with open('services/inventory_metrics.py', 'w') as f:
    f.write(content)

print('Functions removed successfully')
