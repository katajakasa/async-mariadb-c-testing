import logging
import typing
from .exceptions import ConnectionException, QueryException, CursorException
from .converters import sql_to_py_mapping, py_to_sql_mapping
from ._mariadb_async import ffi, lib


def readable_ret(value: int) -> str:
    out = []
    if value & lib.MYSQL_WAIT_READ:
        out.append('MYSQL_WAIT_READ')
    if value & lib.MYSQL_WAIT_WRITE:
        out.append('MYSQL_WAIT_WRITE')
    if value & lib.MYSQL_WAIT_EXCEPT:
        out.append('MYSQL_WAIT_EXCEPT')
    if value & lib.MYSQL_WAIT_TIMEOUT:
        out.append('MYSQL_WAIT_TIMEOUT')
    if not out:
        return 'NULL'
    return '|'.join(out)


class ConnectionBase:
    def __init__(self):
        self.conn = None
        self.log = None
    
    def get_error(self) -> str:
        return ffi.string(lib.mysql_error(self.conn)).decode('utf8', 'surrogateescape')

    def get_errno(self) -> int:
        return lib.mysql_errno(self.conn)

    def exc_build(self, cls):
        raise cls(
            error=self.get_error(),
            code=self.get_errno())

    def wait(self, status: int) -> int:
        ret = lib.wait_for_mysql(self.conn, status)
        self.log.debug("wait(%s) -> %s", readable_ret(status), readable_ret(ret))
        return ret


class Cursor(ConnectionBase):
    def __init__(self, conn, log, result):
        super(Cursor, self).__init__()
        self.log = log
        self.conn = conn
        self.result = result
        self._num_fields = None
        self._field_info = None
        self._row = ffi.new('MYSQL_ROW *')

    def fetch_row_start(self) -> int:
        ret = lib.mysql_fetch_row_start(self._row, self.result)
        self.log.debug("fetch_row_start() -> %s", readable_ret(ret))
        if self.get_errno() != 0:
            self.exc_build(CursorException)
        return ret

    def fetch_row_cont(self, status: int) -> int:
        ret = lib.mysql_fetch_row_start(self._row, self.result, status)
        self.log.debug("fetch_row_start(%s) -> %s", readable_ret(status), readable_ret(ret))
        if self.get_errno() != 0:
            self.exc_build(CursorException)
        return ret

    def free_result_start(self) -> int:
        ret = lib.mysql_free_result_start(self.result)
        self.log.debug("free_result_start() -> %s", readable_ret(ret))
        return ret

    def free_result_cont(self, status: int) -> int:
        ret = lib.mysql_free_result_cont(self.result, status)
        self.log.debug("free_result_cont(%s) -> %s", readable_ret(status), readable_ret(ret))
        return ret

    @property
    def is_finished(self) -> bool:
        return self._row[0] == ffi.NULL

    @property
    def num_fields(self):
        if not self._num_fields:
            self._num_fields = lib.mysql_num_fields(self.result)
        return self._num_fields

    @property
    def field_info(self):
        if not self._field_info:
            self._field_info = []
            c_fields = lib.mysql_fetch_fields(self.result)
            for i in range(self.num_fields):
                f = c_fields[i]
                self._field_info.append((
                    ffi.string(f.name, f.name_length).decode('utf8', 'surrogateescape'), f.type))
        return self._field_info

    @property
    def row(self) -> typing.Tuple[typing.Any]:
        c_lengths = lib.mysql_fetch_lengths(self.result)
        return tuple(
            sql_to_py_mapping[self.field_info[i][1]](
                ffi.unpack(self._row[0][i], c_lengths[i])
            ) for i in range(self.num_fields)
        )


class Connection(ConnectionBase):
    def __init__(self):
        super(Connection, self).__init__()
        self.log = logging.getLogger(__name__)
        self.conn = lib.mysql_init(ffi.NULL)
        lib.mysql_options(self.conn, lib.MYSQL_OPT_NONBLOCK, ffi.NULL)
        lib.mysql_options(self.conn, lib.MYSQL_SET_CHARSET_NAME, "utf8mb4".encode('ascii'))

    def close_start(self) -> int:
        ret = lib.mysql_close_start(self.conn)
        self.log.debug("close_start() -> %s", readable_ret(ret))
        return ret

    def close_cont(self, status: int) -> int:
        ret = lib.mysql_close_cont(self.conn, status)
        self.log.debug("close_cont(%s) -> %s", readable_ret(status), readable_ret(ret))
        return ret

    def connect_start(self, host: str, user: str, passwd: str, db: str, port: int) -> int:
        c_out = ffi.new("MYSQL **")
        c_host = ffi.new("char[]", host.encode('utf8', 'surrogateescape'))
        c_user = ffi.new("char[]", user.encode('utf8', 'surrogateescape'))
        c_passwd = ffi.new("char[]", passwd.encode('utf8', 'surrogateescape'))
        c_db = ffi.new("char[]", db.encode('utf8', 'surrogateescape'))
        ret = lib.mysql_real_connect_start(
            c_out, self.conn, c_host, c_user, c_passwd, c_db, port, ffi.NULL, 0)
        self.log.debug("connect_start(%s, %s, '********', %s, %d) -> %s", host, user, db, port, readable_ret(ret))
        if ret == 0 and c_out[0] == ffi.NULL:
            self.exc_build(ConnectionException)
        return ret

    def connect_cont(self, status: int) -> int:
        c_out = ffi.new("MYSQL **")
        ret = lib.mysql_real_connect_cont(c_out, self.conn, status)
        self.log.debug("connect_cont(%s) -> %s", readable_ret(status), readable_ret(ret))
        if ret == 0 and c_out[0] == ffi.NULL:
            self.exc_build(ConnectionException)
        return ret

    def query_start(self, query: typing.Union[str, bytearray]) -> int:
        if isinstance(query, str):
            query = bytes(query.encode('utf8', 'surrogateescape'))
        elif isinstance(query, bytearray):
            query = bytes(query)

        c_out = ffi.new("int *")
        c_query = ffi.new("char[]", query)
        ret = lib.mysql_real_query_start(c_out, self.conn, c_query, len(query))
        self.log.debug("query_start(%s) -> %s", query, readable_ret(ret))
        if c_out[0] != 0:
            self.exc_build(QueryException)
        return ret

    def query_cont(self, status: int) -> int:
        c_out = ffi.new("int *")
        ret = lib.mysql_real_query_cont(c_out, self.conn, status)
        self.log.debug("query_cont(%s) -> %s", readable_ret(status), readable_ret(ret))
        if c_out[0] != 0:
            self.exc_build(QueryException)
        return ret

    def store_result(self) -> Cursor:
        ret = lib.mysql_store_result(self.conn)
        self.log.debug("store_result() -> %s", ret)
        if ret == ffi.NULL:
            self.exc_build(CursorException)
        return Cursor(self.conn, self.log, ret)

    def use_result(self) -> Cursor:
        ret = lib.mysql_use_result(self.conn)
        self.log.debug("use_result() -> %s", ret)
        if ret == ffi.NULL:
            self.exc_build(CursorException)
        return Cursor(self.conn, self.log, ret)
