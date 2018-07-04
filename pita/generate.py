from random import randint

from pita.models import get_raw_data_count, crete_raw_data_n_rows

ROWS_COUNT_RANGE = (9e3, 1e4)
ROWS_COUNT = randint(*ROWS_COUNT_RANGE)

WORKERS_COUNT = 9


def generate_raw_data():
    rows_count = get_raw_data_count()

    if rows_count > 0:
        return

    crete_raw_data_n_rows(ROWS_COUNT)


if __name__ == "__main__":
    generate_raw_data()
