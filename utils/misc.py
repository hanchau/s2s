import os

class Cleaner:
    def __init__(self, logger):
        self.logger = logger

    def clean(self, *args):
        for arg in args:
            try:
                del arg
            except:
                pass
        self.logger.info(f"Cleaned Unnecesaary Variables.")

    def clean_files(self, *args):
        for arg in args:
            try:
                os.remove(arg)
            except:
                pass
        self.logger.info(f"Cleaned Unnecesaary Files [{args}]")

