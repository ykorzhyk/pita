import logging
import math
from collections import defaultdict

from pita.models import (
    raw_data, get_saved_agg_data, update_user_data, insert_user_data, get_random_users,
    get_calculated_data)

logger = logging.getLogger(__name__)


def agg_raw_data_bulk(bulk):
    res = defaultdict(lambda: defaultdict(int))

    for row in bulk:
        user_id = row[raw_data.c.user_id]
        event_id = row[raw_data.c.event_id]
        amount = row[raw_data.c.amount]

        user_data = res[user_id]

        user_data['balance'] += amount
        user_data['event_number'] += 1

        if amount > user_data.get('best_event_amount', -math.inf):
            user_data['best_event_amount'] = amount
            user_data['best_event'] = event_id

        if amount < user_data.get('worst_event_amount', math.inf):
            user_data['worst_event_amount'] = amount
            user_data['worst_event'] = event_id

    return res


def _update_with_saved_data(data, saved_data):
    if not saved_data:
        return

    for user_id, saved_user_data in saved_data.items():
        data[user_id]['balance'] += saved_user_data['balance']
        data[user_id]['event_number'] += saved_user_data['event_number']

        if saved_user_data['best_event_amount'] > data[user_id]['best_event_amount']:
            data[user_id]['best_event'] = saved_user_data['best_event']
            data[user_id]['best_event_amount'] = saved_user_data['best_event_amount']

        if saved_user_data['worst_event_amount'] < data[user_id]['worst_event_amount']:
            data[user_id]['worst_event'] = saved_user_data['worst_event']
            data[user_id]['worst_event_amount'] = saved_user_data['worst_event_amount']


def save_agg_data(data):
    user_ids = list(data.keys())
    saved_data = get_saved_agg_data(user_ids)

    _update_with_saved_data(data, saved_data)

    for user_id, user_data in data.items():
        if user_id in saved_data:
            update_user_data(user_id, user_data)
        else:
            insert_user_data(user_id, user_data)


def check_aggregation():
    user_ids = get_random_users()
    calculated_data = get_calculated_data(user_ids)
    stored_data = get_saved_agg_data(user_ids)

    for user_id, user_data in calculated_data.items():
        stored_user_data = stored_data[user_id]

        assert user_data['worst_event'] == stored_user_data['worst_event']
        assert user_data['worst_event_amount'] == stored_user_data['worst_event_amount']
        assert user_data['best_event'] == stored_user_data['best_event']
        assert user_data['best_event_amount'] == stored_user_data['best_event_amount']
        assert user_data['balance'] == stored_user_data['balance']
        assert user_data['event_number'] == stored_user_data['event_number']
