import zerorpc
import gevent.queue
import logging
import sys

logger = logging.getLogger("service")


class QueueingLogHandler(logging.Handler):
    """ A simple logging handler which puts all emitted logs into a
        gevent queue.
    """

    def __init__(self, queue, level, formatter):
        super(QueueingLogHandler, self).__init__()
        self._queue = queue
        self.setLevel(level)
        self.setFormatter(formatter)
    
    def emit(self, record):
        msg = self.format(record)
        self._queue.put_nowait(msg)
    
    def close(self):
        super(QueueingLogHandler, self).close()
        self._queue.put_nowait(None)
    
    @property
    def emitted(self):
        return self._queue


class Service(object):
    
    _HANDLER_CLASS = QueueingLogHandler
    _DEFAULT_FORMAT = '%(name)s - %(levelname)s - %(asctime)s - %(message)s'
    
    def __init__(self):
        self._logging_handlers = set()
    
    def available_loggers(self):
        """ List of initalized loggers """
        return logging.getLogger().manager.loggerDict.keys()
    
    def close_log_streams(self):
        """ Closes all log_stream streams. """
        while self._logging_handlers:
            self._logging_handlers.pop().close()

    @zerorpc.stream
    def log_stream(self, logger_name, level_name, format_str):
        """ Attaches a log handler to the specified logger and sends emitted logs 
            back as stream. 
        """
        if logger_name != "" and logger_name not in self.available_loggers():
            raise ValueError("logger {0} is not available".format(logger_name))

        level_name_upper = level_name.upper() if level_name else "NOTSET"
        try:
            level = getattr(logging, level_name_upper)
        except AttributeError, e:
            raise AttributeError("log level {0} is not available".format(level_name_upper))
        
        q = gevent.queue.Queue()
        fmt = format_str if format_str.strip() else self._DEFAULT_FORMAT 
        
        _logger = logging.getLogger(logger_name)
        formatter = logging.Formatter(fmt)
        handler = self._HANDLER_CLASS(q, level, formatter)
        
        _logger.addHandler(handler)
        self._logging_handlers.add(handler)

        logger.debug("new subscriber for {0}/{1}".format(logger_name or "root", level_name_upper))
        try:
            for msg in handler.emitted:
                if msg is None:
                    return
                yield msg
        finally:
            self._logging_handlers.discard(handler)
            handler.close()
            logger.debug("subscription finished for {0}/{1}".format(logger_name or "root", level_name_upper))
