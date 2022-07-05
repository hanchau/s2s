import logging

logging.basicConfig(level=logging.NOTSET)


class Logger:
    def __init__(self, logfile="", _name=""):
        if not logfile:
            self.logger = logging.getLogger(_name)
        else:
            self.logger = logging.getLogger(_name)
            c_handler = logging.StreamHandler()
            f_handler = logging.FileHandler(logfile)

            c_format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
            f_format = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

            c_handler.setFormatter(c_format)
            f_handler.setFormatter(f_format)

            self.logger.addHandler(c_handler)
            self.logger.addHandler(f_handler)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def critical(self, msg):
        self.logger.critical(msg)
