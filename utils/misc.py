from asyncio.log import logger
class Cleaner:
    def __init__(self, logger):
        self.logger = logger

    def clean(self, *args):
        self.logger.info(f"Cleaned Unnecesaary Variables.")
        for arg in args:
            try:
                del arg
            except:
                pass
