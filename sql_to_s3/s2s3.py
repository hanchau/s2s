import os
import yaml
import time
import pandas as pd
from utils.logger import Logger
from utils.misc import Cleaner
from connectors.sql import SQLConnector
from connectors.s3 import S3Connector

CONFIG_PATH = "sql_to_s3/config_s2s3.yml"
LOGFILE = "logs/s2s3.logs"

logger = Logger(logfile=LOGFILE, _name=__name__)


class ConfigTasks:
    def __init__(self, config_path) -> None:
        try:
            with open(config_path, "r") as config:
                self.tasks = yaml.load(config, Loader=yaml.FullLoader)
            logger.info(f"Config loaded successfully")
        except Exception as err:
            logger.error(f"ConfigTasks: {err}")

    def __iter__(self):
        try:
            for task, task_info in self.tasks.items():
                yield task, task_info
        except Exception as err:
            logger.error(f"Task Iteration: {err}")


if __name__ == "__main__":

    import pdb; pdb.set_trace()
    tasks = ConfigTasks(CONFIG_PATH)
    for task, task_info in tasks:
        if task_info.get("skip"):
            continue

        src = task_info.get("source")
        src_conn = src.get("conn")
        src_get_query = src.get("get_query")

        dest = task_info.get("dest")
        bucket = dest.get("bucket")
        file = dest.get("file")
        file_to = dest.get("file_to")
        

        sql = SQLConnector(logger)
        s3 = S3Connector(logger)

        if not s3.download_objects(bucket, file, file_to):
            logger.error(f"Issue in Downloading File from S3:")
            raise Exception("Cant' Move Forward! Aborting!")

        latest_price_data = pd.read_csv(file_to)