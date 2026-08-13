import pytest
import pandas as pd
from dags.include.business_logic.metrics import calculate_daily_revenue

def test_calculate_daily_revenue_success():
    data = [
        {'order_id': 1, 'amount': 100.0, 'status': 'COMPLETED'},
        {'order_id': 2, 'amount': 200.0, 'status': 'COMPLETED'},
        {'order_id': 3, 'amount': 150.0, 'status': 'FAILED'}
    ]
    df = pd.DataFrame(data)
    result = calculate_daily_revenue(df)
    assert result == 300.0

def test_calculate_daily_revenue_empty():
    df = pd.DataFrame()
    assert calculate_daily_revenue(df) == 0.0

def test_calculate_daily_revenue_invalid_amount():
    data = [{'order_id': 1, 'amount': -100.0, 'status': 'COMPLETED'}]
    df = pd.DataFrame(data)
    assert calculate_daily_revenue(df) == 0.0