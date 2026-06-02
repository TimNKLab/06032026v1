with open('services/inventory_metrics.py', 'r') as f:
    lines = f.readlines()

output_lines = []
skip_mode = False
skip_until_next_def = False

for i, line in enumerate(lines):
    # Check if this is the start of _query_sell_through
    if line.strip().startswith('def _query_sell_through('):
        skip_mode = True
        skip_until_next_def = True
        output_lines.append('# _query_sell_through removed - deferred due to complexity\n')
        continue
    
    # Check if this is the start of get_sell_through_analysis
    if line.strip().startswith('def get_sell_through_analysis('):
        skip_mode = True
        skip_until_next_def = True
        output_lines.append('# get_sell_through_analysis removed - depends on _query_sell_through\n')
        continue
    
    # If we're skipping and we hit the next function definition, stop skipping
    if skip_until_next_def and line.strip().startswith('def ') and not line.strip().startswith('def _query_sell_through(') and not line.strip().startswith('def get_sell_through_analysis('):
        skip_mode = False
        skip_until_next_def = False
        output_lines.append(line)
    elif not skip_mode:
        output_lines.append(line)

with open('services/inventory_metrics.py', 'w') as f:
    f.writelines(output_lines)

print('Sell-through functions removed successfully')
