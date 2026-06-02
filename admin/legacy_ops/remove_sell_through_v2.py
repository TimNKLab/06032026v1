import re

with open('services/inventory_metrics.py', 'r') as f:
    content = f.read()

# Find _query_sell_through function (from def to return statement)
# Pattern: def _query_sell_through(...) -> ... return result\n\n
pattern1 = r'\ndef _query_sell_through\(.*?\n(?:.*?\n)*?    return result\n\n'
content = re.sub(pattern1, '# _query_sell_through removed - deferred due to complexity\n# Previously used SQLite MVs which no longer exist\n\n', content, flags=re.DOTALL)

# Find get_sell_through_analysis function
# Pattern: def get_sell_through_analysis(...) -> ... return {...}\n\n
pattern2 = r'\ndef get_sell_through_analysis\(.*?\n(?:.*?\n)*?    return \{[^}]+\}\n\n'
content = re.sub(pattern2, '# get_sell_through_analysis removed - depends on _query_sell_through\n\n', content, flags=re.DOTALL)

with open('services/inventory_metrics.py', 'w') as f:
    f.write(content)

print('Sell-through functions removed successfully')
