import logging


def setup_logging(logging_destination='console', verbosity='INFO'):
    logHandlers = {
        'console': logging.StreamHandler(),
        'none': logging.NullHandler(),
        # 'file': logging.FileHandler('./omni4d.log')
    }
    logHandler = logHandlers[logging_destination]

    logFormatters = {
        'console': '%(message)s',
        'none': '',
        'file': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    }
    logFormatter = logging.Formatter(logFormatters[logging_destination])

    logHandler.setFormatter(logFormatter)
    logger = logging.getLogger('omni4d_python')
    level = logging.getLevelName(verbosity.upper())
    logger.setLevel(level)
    logger.addHandler(logHandler)
    return logger