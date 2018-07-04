import logging

from sqlalchemy import Table, Column, Integer, SmallInteger, ForeignKey, select, UniqueConstraint, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.functions import count, random

from pita.db_connection import metadata, engine, connection
from pita.utils import get_user_id, get_event_id, get_amount, BULK_SIZE

logger = logging.getLogger(__name__)


raw_data = Table(
    'raw_data',
    metadata,
    Column('id', Integer, autoincrement=True, primary_key=True),
    Column('user_id', SmallInteger),
    Column('event_id', SmallInteger),
    Column('amount', Integer),
    UniqueConstraint('user_id', 'event_id', name='unique_user_event')
)

agg_data = Table(
    'agg_data',
    metadata,
    Column('id', Integer, autoincrement=True, primary_key=True),
    Column('user_id', SmallInteger),
    Column('balance', Integer),
    Column('event_number', Integer),
    Column('best_event', SmallInteger),
    Column('worst_event_amount', Integer),
    Column('worst_event', SmallInteger),
    Column('best_event_amount', Integer),
)


last_processed_id = Table(
    'last_processed_id',
    metadata,
    Column('id', Integer, ForeignKey("raw_data.id")),
)


metadata.create_all(engine)


def crete_raw_data_n_rows(n):
    def _generate_row():
        data = {
            'user_id': get_user_id(),
            'event_id': get_event_id(),
            'amount': get_amount()
        }

        return data

    i = n
    unique_index = set()

    while i > 0:
        row = _generate_row()
        unique_key = (row['user_id'], row['event_id'])

        if unique_key in unique_index:
            continue
        else:
            unique_index.add(unique_key)

        try:
            connection.execute(raw_data.insert(), row)
        except IntegrityError:
            unique_index.add(unique_key)
            continue
        else:
            i -= 1

    logger.debug(f"{n} raw_data rows have been generated.")


def get_raw_data_count():
    res = connection.execute(
        count(raw_data.c.id)
    ).fetchone()

    rows_count = res and res[0]

    return rows_count


def get_last_processed_id():
    res = connection.execute(
        select([last_processed_id.c.id])
    ).fetchone()

    last_id = res and res[0] or 0

    return last_id


def save_last_id(last_id, new_last_id):
    if last_id:
        q = last_processed_id.update()
    else:
        q = last_processed_id.insert()

    connection.execute(q.values(id=new_last_id))


def get_raw_data_bulk():
    last_id = get_last_processed_id()
    res = connection.execute(
        select([raw_data]).where(raw_data.c.id > last_id).order_by(raw_data.c.id).limit(BULK_SIZE)
    )

    bulk = res.fetchall()

    if not bulk:
        return

    new_last_id = bulk[-1][raw_data.c.id]

    save_last_id(last_id, new_last_id)

    return bulk


def get_saved_agg_data(user_ids):
    res = connection.execute(select([agg_data]).where(agg_data.c.user_id.in_(user_ids)))

    bulk = res.fetchall()

    if not bulk:
        return {}

    return {
        row[agg_data.c.user_id]: {
            'balance': row[agg_data.c.balance],
            'event_number': row[agg_data.c.event_number],
            'best_event': row[agg_data.c.best_event],
            'worst_event': row[agg_data.c.worst_event],
            'worst_event_amount': row[agg_data.c.worst_event_amount],
            'best_event_amount': row[agg_data.c.best_event_amount]
        } for row in bulk
    }


def update_user_data(user_id, user_data):
    q = agg_data.update().where(agg_data.c.user_id == user_id).values(**user_data)
    connection.execute(q)


def insert_user_data(user_id, user_data):
    q = agg_data.insert().values(user_id=user_id, **user_data)
    connection.execute(q)


def get_random_users(n=10):
    res = connection.execute(select([agg_data.c.user_id]).order_by(random()).limit(n))

    data = res.fetchall()

    return [i[0] for i in data]


def get_calculated_data(user_ids):
    sql = text("""
    with subquery as  (select *
    from (
           select
             user_id,
             event_id,
             amount,
             row_number() OVER (PARTITION BY user_id ORDER BY amount ) AS best_events,
             row_number() OVER (PARTITION BY user_id ORDER BY amount desc ) AS worst_events,
             sum(amount) OVER (PARTITION BY user_id ) AS balance
           from raw_data
           where user_id in :user_ids
         ) as a
    where best_events = 1 or worst_events = 1)

    select
      q1.user_id as user_id,
      q1.event_id as worst_event,
      q1.amount as worst_amount,
      q2.event_id as best_event,
      q2.amount as best_amount,
      q1.balance as balance,
      q1.worst_events as event_number
    from subquery q1 inner join subquery q2 on q1.user_id = q2.user_id and q1.best_events = 1 and q2.worst_events = 1;
    """)

    res = connection.execute(sql, user_ids=tuple(user_ids))
    data = res.fetchall()
    columns = (
        'user_id', 'worst_event', 'worst_event_amount', 'best_event', 'best_event_amount', 'balance', 'event_number'
    )

    data_dict = [dict(zip(columns, row)) for row in data]

    return {
        d['user_id']: d
        for d in data_dict
    }
