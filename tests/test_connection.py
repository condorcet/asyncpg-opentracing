import pytest
from operator import itemgetter
import asyncio

import opentracing
from opentracing.scope_managers.asyncio import AsyncioScopeManager
from opentracing.mocktracer import MockTracer

from asyncpg.exceptions import InterfaceError, PostgresSyntaxError,\
    PostgresError
from asyncpg.connection import connect, Connection
from asyncpg.pool import create_pool

from asyncpg_opentracing import tracing_connection


pytestmark = pytest.mark.asyncio


def tags(statement, params=None, error=False):
    _tags = {
        'db.type': 'SQL',
        'db.statement': statement,
        'db.user': 'postgres',
        'db.instance': 'test_db',
        'db.params': params if params else (),
        'span.kind': 'client',
    }
    if error:
        _tags['error'] = True
    return _tags


def error_log(exc):
    return {
        'event': 'error',
        'error.kind': type(exc.value).__name__,
        'error.object': exc.value,
        'message': str(exc.value)
    }


def parent_of(parent, *children):
    for child in children:
        assert parent.context.span_id == child.parent_id


@pytest.fixture(autouse=True)
def tracer():
    tracer = MockTracer(scope_manager=AsyncioScopeManager())
    opentracing.set_global_tracer(tracer)
    return tracer


class FinishedSpans:

    def __init__(self, tracer, conn, filter_reset_query=True):
        self.tracer = tracer
        self.conn = conn
        self.filter_reset_query = filter_reset_query

    def __call__(self):
        # Filter spans that collected when releasing connection pool.
        if self.filter_reset_query:
            return [
                span for span in self.tracer.finished_spans()
                if span.tags.get('db.statement') !=
                self.conn._get_reset_query()
            ]
        else:
            return self.tracer.finished_spans()

    def __getitem__(self, operation_name):
        return self.name(operation_name)

    @property
    def last(self):
        return self()[-1]

    @property
    def count(self):
        return len(self())

    def names(self, *operation_names):
        return [
            span for span in self()
            if span.operation_name in operation_names
        ]

    def name(self, operation_name):
        try:
            return self.names(operation_name)[0]
        except IndexError:
            return None


@pytest.fixture
def finished_spans(tracer, conn):
    return FinishedSpans(tracer, conn)


async def _prepare_schema(conn):
    await conn.execute('''
        CREATE TABLE test_table (
          id SERIAL PRIMARY KEY,
          name varchar
        );
    ''')


async def _prepare_data(conn):
    await conn.execute('''
        INSERT INTO test_table VALUES (1, 'foo'), (2 ,'bar'), (3, 'hello')
    ''')


@pytest.fixture(autouse=True)
async def prepare_db():
    conn = await connect('postgres://postgres@/postgres')
    try:
        await conn.execute('CREATE DATABASE test_db')
    except PostgresError:
        # Recreate database if it's already exists.
        await conn.execute('DROP DATABASE test_db')
        await conn.execute('CREATE DATABASE test_db')
    # Create schema.
    test_db_conn = await connect('postgres://postgres@/test_db')
    await _prepare_schema(test_db_conn)
    await test_db_conn.close()
    yield
    await conn.execute('DROP DATABASE test_db')
    await conn.close()


@pytest.fixture
async def prepare_data(dsn):
    conn = await connect(dsn)
    await _prepare_data(conn)
    await conn.close()


@pytest.fixture
def dsn():
    return 'postgres://postgres@/test_db'


@pytest.fixture
def connection_class():
    @tracing_connection
    class TracedConnection(Connection):
        pass
    return TracedConnection


@pytest.fixture
async def conn(dsn, connection_class):
    conn = await connect(dsn, connection_class=connection_class)
    yield conn
    await conn.close()


@pytest.fixture
async def pool_conn(dsn, connection_class):
    conn = await create_pool(dsn, connection_class=connection_class)
    yield conn
    await conn.close()


def combined_conn():
    return pytest.mark.parametrize(
        'combined_conn', [
            pytest.lazy_fixture('conn'),
            pytest.lazy_fixture('pool_conn'),
        ], )


@combined_conn()
# TODO: parametrize with `UniqueViolationError` etc.
async def test_sql_exception(combined_conn, finished_spans):
    conn = combined_conn
    assert finished_spans.count == 0
    with pytest.raises(PostgresSyntaxError) as exc:
        await conn.execute('foobar;')
    assert str(exc.value) == 'syntax error at or near "foobar"'
    assert finished_spans.count == 1
    span = finished_spans.last
    assert span.operation_name == 'asyncpg FOOBAR'
    assert span.tags == tags(statement='foobar;', error=True)
    assert span.logs[0].key_values == error_log(exc)


@combined_conn()
async def test_connection_exception(combined_conn, finished_spans):
    conn = combined_conn
    assert finished_spans.count == 0
    await conn.close()
    with pytest.raises(InterfaceError) as exc:
        await conn.fetch('SELECT 1;')

    if isinstance(conn, Connection):
        assert str(exc.value) == 'connection is closed'
        assert finished_spans.count == 1
        span = finished_spans.last
        assert span.operation_name == 'asyncpg SELECT'
        assert span.tags == tags(statement='SELECT 1;', error=True)
        assert len(span.logs) == 2
        assert span.logs[0].key_values == error_log(exc)
    else:
        assert str(exc.value) == 'pool is closed'
        assert finished_spans.count == 0


@combined_conn()
@pytest.mark.parametrize('statement, params, operation_name', [
    ('SELECT * from test_table', (), 'asyncpg SELECT'),
    ('INSERT INTO test_table VALUES (4, \'foobar\')', (), 'asyncpg INSERT'),
    ('DELETE FROM test_table', (), 'asyncpg DELETE'),
    ('SELECT FROM test_table WHERE name=$1', ('foo', ), 'asyncpg SELECT'),
    ('DELETE FROM test_table WHERE name=$1', ('foo', ), 'asyncpg DELETE'),
])
async def test_execute(
        combined_conn, finished_spans, statement, operation_name, params,
        prepare_data
):
    conn = combined_conn
    assert finished_spans.count == 0
    await conn.execute(statement, *params)
    assert finished_spans.count == 1
    span = finished_spans.last
    assert span.operation_name == operation_name
    assert span.tags == tags(statement=statement, params=params)
    assert len(span.logs) == 0


@combined_conn()
async def test_fetch(combined_conn, finished_spans, prepare_data):
    conn = combined_conn
    assert finished_spans.count == 0

    statement = 'SELECT * FROM test_table WHERE id >= $1 ORDER BY id'
    params = (2, )
    res = await conn.fetch(statement, *params)
    assert res == [(2, 'bar'), (3, 'hello')]
    assert finished_spans.count == 1
    span = finished_spans.last
    assert span.operation_name == 'asyncpg SELECT'
    assert span.tags == tags(statement=statement, params=params)
    assert len(span.logs) == 0


@combined_conn()
async def test_fetchrow(combined_conn, finished_spans, prepare_data):
    conn = combined_conn
    assert finished_spans.count == 0

    statement = 'SELECT * FROM test_table WHERE id >= $1 ORDER BY id'
    params = (2, )
    res = await conn.fetchrow(statement, *params)
    assert res == (2, 'bar')
    assert finished_spans.count == 1
    span = finished_spans.last
    assert span.operation_name == 'asyncpg SELECT'
    assert span.tags == tags(statement=statement, params=params)
    assert len(span.logs) == 0


@combined_conn()
async def test_fetchval(combined_conn, finished_spans, prepare_data):
    conn = combined_conn
    assert finished_spans.count == 0

    statement = 'SELECT * FROM test_table WHERE id >= $1 ORDER BY id'
    params = (2, )
    res = await conn.fetchval(statement, *params)
    assert res == 2
    assert finished_spans.count == 1
    span = finished_spans.last
    assert span.operation_name == 'asyncpg SELECT'
    assert span.tags == tags(statement=statement, params=params)
    assert len(span.logs) == 0


@combined_conn()
async def test_executemany(combined_conn, finished_spans):
    conn = combined_conn
    assert finished_spans.count == 0

    statement = 'INSERT INTO test_table (name) VALUES ($1)'
    params = [('zz', ), ('top', )]
    await conn.executemany(statement, params)
    assert finished_spans.count == 1
    span = finished_spans.last
    assert span.operation_name == 'asyncpg INSERT'
    assert span.tags == tags(statement=statement, params=params)
    assert len(span.logs) == 0


@combined_conn()
async def test_nested_spans(combined_conn, tracer, finished_spans):
    conn = combined_conn
    assert finished_spans.count == 0
    with tracer.start_active_span('root', ignore_active_span=True):
        await conn.execute('INSERT INTO test_table VALUES (1, \'test\')')
        with tracer.start_active_span('fetch'):
            await conn.fetchrow('SELECT FROM test_table WHERE id=1')
        with tracer.start_active_span('remove'):
            await conn.fetchrow('DELETE FROM test_table WHERE id=1')
    assert finished_spans.count == 6
    root_span = finished_spans['root']
    fetch_span = finished_spans['fetch']
    insert_span = finished_spans['asyncpg INSERT']
    select_span = finished_spans['asyncpg SELECT']
    remove_span = finished_spans['remove']
    delete_span = finished_spans['asyncpg DELETE']

    parent_of(root_span, insert_span, fetch_span, remove_span)
    parent_of(fetch_span, select_span)
    parent_of(remove_span, delete_span)

    assert insert_span.tags == tags(
        statement='INSERT INTO test_table VALUES (1, \'test\')')
    assert len(insert_span.logs) == 0

    assert select_span.tags == tags(
        statement='SELECT FROM test_table WHERE id=1')
    assert len(select_span.logs) == 0

    assert delete_span.tags == tags(
        statement='DELETE FROM test_table WHERE id=1')
    assert len(delete_span.logs) == 0


async def test_transaction_commit(conn, finished_spans):
    assert finished_spans.count == 0

    async with conn.transaction():
        await conn.execute('DELETE FROM test_table')
    assert finished_spans.count == 3

    assert finished_spans['asyncpg BEGIN']
    assert finished_spans['asyncpg DELETE']
    assert finished_spans['asyncpg COMMIT']

    delete = finished_spans['asyncpg DELETE']
    assert delete.tags == tags(statement='DELETE FROM test_table')
    assert len(delete.logs) == 0


async def test_transaction_rollback(conn, finished_spans):
    assert finished_spans.count == 0

    with pytest.raises(ZeroDivisionError):
        async with conn.transaction():
            await conn.execute('DELETE FROM test_table')
            1 / 0
    assert finished_spans.count == 3
    assert finished_spans['asyncpg BEGIN']
    assert finished_spans['asyncpg DELETE']
    assert finished_spans['asyncpg ROLLBACK']

    delete = finished_spans['asyncpg DELETE']
    assert delete.tags == tags(statement='DELETE FROM test_table')
    assert len(delete.logs) == 0


async def test_concurrent_coroutines(pool_conn, tracer, finished_spans):
    conn = pool_conn

    async def task(span, name):
        with tracer.scope_manager.activate(span, False):
            await conn.execute(
                'INSERT INTO test_table (name) VALUES ($1)', name
            )

    assert finished_spans.count == 0
    with tracer.start_active_span('root', ignore_active_span=True) as scope:

        await asyncio.gather(
            task(scope.span, 'task1'),
            task(scope.span, 'task2'),
            task(scope.span, 'task3'),
        )
        assert await conn.fetchval('SELECT count(*) from test_table') == 3

    assert finished_spans['root']

    insert_spans = finished_spans.names('asyncpg INSERT')
    assert len(insert_spans) == 3
    parent_of(finished_spans['root'], *insert_spans)

    insert_tags = []
    insert_logs = []
    # sort span by args.
    for span in insert_spans:
        insert_tags.append(span.tags)
        insert_logs.append(len(span.logs))
    insert_tags.sort(key=itemgetter('db.params'))

    assert insert_tags == [
        tags(
            statement='INSERT INTO test_table (name) VALUES ($1)',
            params=(task, )
        ) for task in ('task1', 'task2', 'task3', )
    ]

    assert insert_logs == [0, 0, 0]
