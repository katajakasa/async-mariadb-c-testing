
class DBException(Exception):
    def __init__(self, error, code):
        self.error = error
        self.code = code
        super(DBException, self).__init__("[{}] {}".format(code, error))


class ConnectionException(DBException):
    pass


class QueryException(DBException):
    pass


class CursorException(DBException):
    pass
