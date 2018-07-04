from pita.aggregate import start_aggregation
from pita.check import run_check_aggregation
from pita.generate import generate_raw_data


def test_generate_raw_data():
    generate_raw_data()


def test_agg_data():
    start_aggregation()


def test_check():
    run_check_aggregation()