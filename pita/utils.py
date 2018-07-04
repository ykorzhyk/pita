from functools import partial
from random import randint

ID_RANGE = (1, 100)
AMOUNT_RANGE = (-1e5, 1e5)

BULK_SIZE = 1e2

get_event_id = partial(randint, *ID_RANGE)
get_user_id = partial(randint, *ID_RANGE)
get_amount = partial(randint, *AMOUNT_RANGE)
