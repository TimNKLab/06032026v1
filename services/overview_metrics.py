from datetime import date, timedelta
from typing import Dict, Tuple

from services.pos_data import get_pos_order_lines_for_date


def _extract_many2one_name(value):
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        return value[1]
    return value or 'Unknown'


def _summaries_for_lines(lines):
    total_amount = 0
    total_qty = 0
    amount_by_category: Dict[str, float] = {}

    for line in lines:
        amount = float(line.get('price_subtotal_incl') or 0)
        qty = float(line.get('qty') or 0)
        category_name = _extract_many2one_name(line.get('product_categ_id'))

        total_amount += amount
        total_qty += qty

        amount_by_category[category_name] = amount_by_category.get(category_name, 0) + amount

    return total_amount, total_qty, amount_by_category


def get_total_overview_summary(target_date: date) -> Dict:
    if not isinstance(target_date, date):
        target_date = date.today()

    today_lines = get_pos_order_lines_for_date(target_date)
    prev_lines = get_pos_order_lines_for_date(target_date - timedelta(days=1))

    today_amount, today_qty, today_categories = _summaries_for_lines(today_lines)
    prev_amount, *_ = _summaries_for_lines(prev_lines)

    return {
        'target_date': target_date,
        'today_amount': today_amount,
        'today_qty': today_qty,
        'prev_amount': prev_amount,
        'categories': today_categories,
    }
