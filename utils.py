import logging


class CustomFormatter(logging.Formatter):
    reset = "\x1b[0m"
    format = "%(asctime)s | %(levelname)-8s | %(message)s | (%(filename)s:%(lineno)d)"

    # Monokai-inspired color codes
    white = "\x1b[38;5;231m"
    grey = "\x1b[38;5;59m"
    blue = "\x1b[38;5;81m"
    green = "\x1b[38;5;148m"
    orange = "\x1b[38;5;208m"
    purple = "\x1b[38;5;141m"
    red = "\x1b[38;5;197m"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: white + format + reset,
        logging.WARNING: orange + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: purple + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, "%Y-%m-%d %H:%M:%S")
        return formatter.format(record)
