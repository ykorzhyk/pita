import logging

from pita.helpers import agg_raw_data_bulk, save_agg_data
from pita.models import get_raw_data_bulk


logger = logging.getLogger(__name__)


def agg_bulk():
    bulk = get_raw_data_bulk()

    if not bulk:
        logger.debug("All raw_data table is processed.")
        return

    agg_data = agg_raw_data_bulk(bulk)

    save_agg_data(agg_data)

    return True


def start_aggregation():
    while True:
        if not agg_bulk():
            break


if __name__ == "__main__":
    start_aggregation()
