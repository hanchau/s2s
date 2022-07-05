import os
from numpy import delete
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


class DataLogics:
    def conditional_price_selector(df1, df2):
        try:
            joined = df1.join(df2, lsuffix="_a", rsuffix="_b").dropna()
            df = pd.DataFrame(joined)
            logger.info(f"\n -----\n{df.head()}")
            df["product"] = df["product_a"]
            for row in df.iterrows():
                if df.at[row[0], "status"] == "inactive":
                    df.at[row[0], "last_price"] = joined.at[row[0], "price"]
                else:
                    df.at[row[0], "last_price"] = joined.at[row[0], "last_price"]
            ret_df = df[["product", "last_price"]]
            logger.info(f"\n -----\n{ret_df.head()}")
            return ret_df.reset_index(drop=True)
        except Exception as err:
            logger.error(f"Error in applying conditional price logic.")


if __name__ == "__main__":
    start_time = time.time()

    tasks = ConfigTasks(CONFIG_PATH)
    task_info = tasks.tasks.get("task1")
    src = task_info.get("source")
    src_conn = src.get("conn")
    src_get_query = src.get("get_query")

    src2 = task_info.get("source2")
    bucket = src2.get("bucket")
    file_s3 = src2.get("file_s3")
    file_loc = src2.get("file_loc")

    dest = task_info.get("dest")
    dest_bucket = dest.get("bucket")
    dest_file_s3 = dest.get("file_s3")
    dest_file_loc = dest.get("file_loc")

    cleaner = Cleaner(logger)
    sql = SQLConnector(logger)
    s3 = S3Connector(logger)

    if not s3.download_objects(bucket, file_s3, file_loc):
        logger.error(f"Issue in Downloading File from S3:")
        raise Exception("Cant' Move Forward! Aborting!")
    latest_price_data = pd.read_csv(file_loc)

    conn = sql.connect(**src_conn)
    product_status = pd.read_sql(src_get_query, conn)

    updated_price = DataLogics.conditional_price_selector(
        latest_price_data, product_status
    )
    updated_price.to_csv(dest_file_loc)

    s3.delete_object(bucket, dest_file_s3)  # old file
    s3.upload_objects(dest_bucket, dest_file_loc, dest_file_s3)  # new file

    logger.info(f"Confirming Objects on S3.")
    s3.list_objects(bucket)
    logger.info(f"Process finished. Total time taken: [{time.time() - start_time}]")

    # cleaner.clean_files(file_s3, file_loc, dest_file_s3, dest_file_loc)