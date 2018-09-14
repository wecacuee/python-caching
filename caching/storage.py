import os
import sqlite3
from contextlib import suppress
from typing import Generator, Tuple, Union, ByteString


class CacheStorageBase:

    def __init__(self, *, maxsize: int, ttl: Union[int, float], policy: str):
        self.maxsize = maxsize
        self.ttl = ttl
        self.policy = policy

    def __setitem__(self, key: ByteString, value: ByteString) -> None:
        raise NotImplementedError  # pragma: no cover

    def __getitem__(self, key) -> bytes:
        raise NotImplementedError  # pragma: no cover

    def __delitem__(self, key) -> None:
        raise NotImplementedError  # pragma: no cover

    def get(self, key: ByteString, default=None) -> Union[bytes, None]:
        raise NotImplementedError  # pragma: no cover

    def clear(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def remove(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def items(self) -> Generator[Tuple[bytes, bytes], None, None]:
        raise NotImplementedError  # pragma: no cover


class SQLiteStorage(CacheStorageBase):
    SQLITE_TIMESTAMP = "(julianday('now') - 2440587.5)*86400.0"
    POLICIES = {
        'FIFO': {
            'additional_columns': (),
            'after_get_ok': None,
            'additional_indexes': (),
            'delete_order_by': 'ts',
        },
        'LRU': {
            'additional_columns': ('used INT NOT NULL DEFAULT 0',),
            'additional_indexes': ('used, ts',),
            'after_get_ok': 'UPDATE cache SET used = (SELECT max(used) FROM cache) + 1',
            'delete_order_by': 'used, ts',
        },
        'LFU': {
            'additional_columns': ('used INT NOT NULL DEFAULT 0',),
            'additional_indexes': ('used, ts',),
            'after_get_ok': 'UPDATE cache SET used = used + 1',
            'delete_order_by': 'used, ts',
        },
    }

    def __init__(self, *, filepath, ttl, maxsize, policy='FIFO'):
        if policy not in self.POLICIES:
            raise ValueError('Invalid policy: {policy}'.format(policy=policy))
        super(SQLiteStorage, self).__init__(
            ttl=ttl, maxsize=maxsize, policy=policy,
        )
        self.filepath = filepath
        self.db = sqlite3.connect(filepath, isolation_level='DEFERRED')
        self.init_db()
        self.nothing = object()

        if self.ttl > 0:
            ttl_filter = '({self.SQLITE_TIMESTAMP} - ts) <= {self.ttl}'.format(self=self)
        else:
            ttl_filter = '1=1'

        self.sql_select = 'SELECT value FROM cache WHERE key = ? AND {ttl_filter}'.format(
            ttl_filter=ttl_filter)
        self.sql_select_kv = 'SELECT key, value FROM cache WHERE {ttl_filter} ORDER BY ts'.format(
            ttl_filter=ttl_filter)
        self.sql_delete = 'DELETE FROM cache WHERE key = ?'
        self.sql_insert = (
            'INSERT OR REPLACE INTO cache (key, value) VALUES (?, ?)'
        )
        after_get_ok = self.POLICIES[self.policy]['after_get_ok']
        if after_get_ok:
            self.sql_after_get_ok = '{after_get_ok} WHERE key = ?'.format(
                after_get_ok=after_get_ok)
        else:
            self.sql_after_get_ok = None

    def close(self):
        self.db.close()

    def __repr__(self):
        params = (
            (p, getattr(self, p))
            for p in ('filepath', 'maxsize', 'ttl')
        )
        return (
            '{self.__class__.__name__}'.format(self=self) +
            "({})".format(', '.join(
                '{k}={v}'.format(k=k, v=repr(v)) for k,v in params))
        )

    def __enter__(self):
        self.init_db()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __setitem__(self, key, value):
        with self.db as db:
            db.execute(self.sql_insert, (key, value))

    def __getitem__(self, key):
        res = self.get(key, None)
        if res is None:
            raise KeyError('Not found')
        else:
            return res

    def __delitem__(self, key):
        cursor = self.db.execute(self.sql_delete, (key,))
        if cursor.rowcount == 0:
            raise KeyError('Not found')

    def get(self, key, default=None):
        with self.db:
            rows = self.db.execute(
                self.sql_select,
                (key,),
            ).fetchall()
            if rows:
                if self.sql_after_get_ok:
                    self.db.execute(self.sql_after_get_ok, (key,))
                return rows[0][0]
            else:
                return default

    def init_db(self):
        policy_stuff = self.POLICIES[self.policy]

        after_insert_actions = []
        if self.ttl > 0:
            after_insert_actions.append('''
                DELETE FROM cache WHERE
                ({self.SQLITE_TIMESTAMP} - ts) > {self.ttl};
            '''.format(self=self))
        if self.maxsize > 0:
            after_insert_actions.append('''
                DELETE FROM cache WHERE key in (
                    SELECT key FROM cache
                    ORDER BY {policy_stuff[delete_order_by]}
                    LIMIT max(0, (SELECT COUNT(key) FROM cache) - {self.maxsize})
                );
            '''.format(self=self, policy_stuff=policy_stuff))

        with self.db as db:
            db.execute('''
                CREATE TABLE IF NOT EXISTS cache (
                    key BINARY PRIMARY KEY,
                    ts REAL NOT NULL DEFAULT ({self.SQLITE_TIMESTAMP}),
                    {x}
                    value BLOB NOT NULL
                ) WITHOUT ROWID
            '''.format(self=self,
                       x=''.join(
                           "{c}, ".format(c=c) for c in policy_stuff['additional_columns'])))
            db.execute('CREATE INDEX IF NOT EXISTS i_cache_ts ON cache (ts)')

            for i, columns in enumerate(policy_stuff['additional_indexes']):
                db.execute(
                    'CREATE INDEX IF NOT EXISTS i_cache_{i} ON cache ({columns})'.format(
                        i=i, columns=columns))

            if after_insert_actions:
                db.execute('''
                    CREATE TRIGGER IF NOT EXISTS t_cache_cleanup
                    AFTER INSERT ON cache FOR EACH ROW BEGIN
                        %s
                    END
                ''' % '\n'.join(after_insert_actions))

    def clear(self):
        with self.db as db:
            db.execute('DROP TABLE IF EXISTS cache')
            db.execute('VACUUM')
        self.init_db()

    def items(self):
        cursor = self.db.execute(self.sql_select_kv)
        try:
            yield from cursor
        finally:
            cursor.close()

    def remove(self):
        self.close()
        with suppress(FileNotFoundError):
            os.remove(self.filepath)
