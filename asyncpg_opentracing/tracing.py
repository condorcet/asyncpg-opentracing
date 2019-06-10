from functools import wraps
from opentracing import global_tracer, tags, logs
from contextlib import contextmanager


def operation_name(query: str):
    # TODO: some statement should contain two words. For example CREATE TABLE.
    query = query.strip().split(' ')[0].strip(';').upper()
    return 'asyncpg ' + query


@contextmanager
def con_context(handler, query, query_args):
    _tags = {
        tags.DATABASE_TYPE: 'SQL',
        tags.DATABASE_STATEMENT: query,
        tags.DATABASE_USER: handler._params.user,
        tags.DATABASE_INSTANCE: handler._params.database,
        'db.params': query_args,
        tags.SPAN_KIND: tags.SPAN_KIND_RPC_CLIENT,
    }
    with global_tracer().start_active_span(
            operation_name=operation_name(query),
            tags=_tags
    ) as scope:
        try:
            yield
        except Exception as e:
            scope.span.log_kv({
                logs.EVENT: 'error',
                logs.ERROR_KIND: type(e).__name__,
                logs.ERROR_OBJECT: e,
                logs.MESSAGE: str(e)
            })
            raise


def wrap(coro):

    @wraps(coro)
    async def wrapped(self, query, *args, **kwargs):
        with con_context(self, query, args):
            return await coro(self, query, *args, **kwargs)
    return wrapped


def _wrap(coro):

    @wraps(coro)
    async def wrapped(self, query, *args, **kwargs):
        _tags = {
            tags.DATABASE_TYPE: 'SQL',
            tags.DATABASE_STATEMENT: query,
            tags.DATABASE_USER: self._params.user,
            tags.DATABASE_INSTANCE: self._params.database,
            'db.params': args,
            tags.SPAN_KIND: tags.SPAN_KIND_RPC_CLIENT,
        }
        with global_tracer().start_active_span(
                operation_name=operation_name(query),
                tags=_tags
        ) as scope:
            try:
                return await coro(self, query, *args, **kwargs)
            except Exception as e:
                scope.span.log_kv({
                    logs.EVENT: 'error',
                    logs.ERROR_KIND: type(e).__name__,
                    logs.ERROR_OBJECT: e,
                    logs.MESSAGE: str(e)
                })
                raise
    return wrapped


def wrap_executemany(coro):

    @wraps(coro)
    async def wrapped(self, query, args, *_args, **kwargs):
        with con_context(self, query, args):
            return await coro(self, query, args, *_args, **kwargs)
    return wrapped


def tracing_connection(cls):

    cls.fetch = wrap(cls.fetch)
    cls.fetchval = wrap(cls.fetchval)
    cls.fetchrow = wrap(cls.fetchrow)
    cls.execute = wrap(cls.execute)
    cls.executemany = wrap_executemany(cls.executemany)

    return cls
