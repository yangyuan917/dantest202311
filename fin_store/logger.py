import os
import datetime
import traceback

from logging import StreamHandler, getLogger, Formatter, DEBUG, INFO, WARNING, ERROR
from logging.handlers import RotatingFileHandler


class Logger(object):

    def __init__(self, name, msg_fmt, file=None, path=None, level=None):
        self.name = name
        self.file = file
        self.msg_fmt = msg_fmt
        self.path = path or os.path.join(os.getcwd(), 'logs')
        self.level = level or DEBUG
        self.set_level(level, name)

    def set_level(self, level, name=None):
        getLogger(name or self.name).setLevel(level)

    def set_file(self, name=None, msg_fmt=None, file=None, path=None,
                 max_bytes=20 * 1024 * 1024, backup_count=10):
        path = path or self.path
        file = file or self.file or self.file_name(name or self.name)

        os.makedirs(path, exist_ok=True)
        handler = RotatingFileHandler(filename=os.path.join(path, file), maxBytes=max_bytes, backupCount=backup_count)
        handler.setFormatter(Formatter(msg_fmt or self.msg_fmt))
        getLogger(name or self.name).addHandler(handler)

    def set_stream(self, name=None, msg_fmt=None):
        handler = StreamHandler()
        handler.setFormatter(Formatter(msg_fmt or self.msg_fmt))
        getLogger(name or self.name).addHandler(handler)

    def set_error_file(self, name=None, msg_fmt=None, file=None, path=None,
                       max_bytes=20 * 1024 * 1024, backup_count=10):
        path = path or self.path
        name = self.error_logger_name(name)
        file = file or self.file or self.file_name(name)

        os.makedirs(path, exist_ok=True)
        handler = RotatingFileHandler(filename=os.path.join(path, file), maxBytes=max_bytes, backupCount=backup_count)
        handler.setFormatter(Formatter(msg_fmt or self.msg_fmt))
        getLogger(name).addHandler(handler)

    def error(self, msg, *args, name=None):
        text = '{} {}\n{}{}'.format(msg, ''.join([str(m) for m in args]),
                                    traceback.format_exc(), ''.join(traceback.format_stack()[:-1]))
        self.write(text, name=name or self.name, level=ERROR)
        getLogger(self.error_logger_name(name)).error(text)

    def write(self, msg, *args, name=None, level=None):
        getLogger(name or self.name).log(level or self.level,
                                         '{} {}'.format(msg, ' '.join([str(m) for m in args])))

    def debug(self, msg, *args, name=None):
        self.write(msg, *args, name=name, level=DEBUG)

    def warning(self, msg, *args, name=None):
        self.write(msg, *args, name=name, level=WARNING)

    def info(self, msg, *args, name=None):
        self.write(msg, *args, name=name, level=INFO)

    def timer(self, name, *args):
        return Timer(name, *args, log_func=self.write)

    def error_logger_name(self, name):
        return name or '{}_error'.format(self.name)

    @staticmethod
    def file_name(name=None):
        return '{}.{:%m%d%H%M}.{}.log'.format(name, datetime.datetime.now(), os.getpid())


class Timer:

    def __init__(self, name, *args, log_func=None):
        self.name = '{} {}'.format(name, ' '.join([str(a) for a in args]))
        self.func = log_func or logger.write
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.datetime.now()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.timing('finish')

    def timing(self, text=''):
        delta = datetime.datetime.now() - self.start_time
        self.func('timing: {0}. {1} {2}.'.format(delta, self.name, text))
        return delta


def set_level(level, name=None):
    logger.set_level(level=level, name=name)


def write_error(msg, *args, name=None):
    logger.error(msg, *args, name=name)


def write_log(msg, *args, name=None):
    logger.write(msg, *args, name=name)


def write_info(msg, *args, name=None):
    logger.info(msg, *args, name=name)


def write_warning(msg, *args, name=None):
    logger.warning(msg, *args, name=name)


def write_timer(name, *args):
    return logger.timer(name, *args)


message_format = "%(asctime)s %(thread)d-%(threadName)-12s %(name)-10s %(levelname)-6s %(message)s"

logger = Logger(name='default', msg_fmt=message_format, level=DEBUG)
logger.set_stream()
logger.set_file()
logger.set_error_file()

