from database.o_db_constants import OConst

__author__ = 'daill'


class ProfileNotMatchException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class NotConnectedException(Exception):
    def __init__(self, msg, error_data):
        self.__error_data = error_data
        self.__msg = msg

    def __str__(self):
        error_msg = "{}:\n".format(self.__msg)
        exceptions = self.__error_data[OConst.EXCEPTION]
        for exception_dict in exceptions:
            error_msg += "{}: {}\n".format(exception_dict[OConst.EXCEPTION_CLASS.value].decode('utf-8'),
                                           exception_dict[OConst.EXCEPTION_MESSAGE.value].decode('utf-8'))
        return error_msg


class WrongTypeException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class OPyClientException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class SQLCommandException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class ParsingError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)