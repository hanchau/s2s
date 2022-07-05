import yaml
import time
from utils.logger import Logger
from utils.misc import Cleaner
from connectors.sql import SQLConnector
from datetime import datetime, timedelta

CONFIG_PATH = "sql_to_sql/config_sql_to_sql.yml"
LOGFILE = "logs/sql_to_sql.logs"

logger = Logger(logfile=LOGFILE, _name=__name__)


class Transformer:
    def format_time(query, time_frame):
        try:
            start = time_frame.get("start")
            end = time_frame.get("end")
            if time_frame.get("yday"):
                end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                start = (datetime.now() - timedelta(days=1)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            formated_query = query % {"start_time": start, "end_time": end}
            return formated_query
        except Exception as err:
            logger.error(f"Format Time: {err}")

    def format_rec(query, record):
        try:
            formated_query = (query % record).replace("\n", "")
            return formated_query
        except Exception as err:
            logger.error(f"Format Record: {err}")


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


class Uploader:
    def dump_records(conn, insert_query, records):
        total_rec = len(records)
        with conn.cursor() as cursor:
            for record in records:
                formated_query = Transformer.format_rec(insert_query, record)
                cursor.execute(formated_query.replace("\n", " "))
        conn.commit()
        del cursor
        return True, total_rec


if __name__ == "__main__":

    tasks = ConfigTasks(CONFIG_PATH)
    for task, task_info in tasks:
        if task_info.get("skip"):
            continue

        src = task_info.get("source")
        src_conn = src.get("conn")
        src_get_query = src.get("get_query")
        src_time_frame = src.get("time_frame")

        dest = task_info.get("dest")
        dest_conn = dest.get("conn")
        dest_check_query = dest.get("check_query")
        dest_create_query = dest.get("create_query")
        dest_insert_query = dest.get("insert_query")
        logger.info(f"Started Source Stage.")
        sql = SQLConnector(logger)
        cleaner = Cleaner(logger)

        try:
            start_time = time.time()
            conn = sql.connect(**src_conn)
            src_get_query = Transformer.format_time(src_get_query, src_time_frame)
            records = sql.fire_query(conn, src_get_query, rec_in_json=True)
            total_time = time.time() - start_time
            logger.info(
                f"Records fetching complete. Total Records Fetched: [{len(records)}]. Total Time: [{total_time}]"
            )
        except Exception as err:
            logger.error(f"Source Stage Error: {err}")
            raise Exception("Can't move forward")

        cleaner.clean(conn, src_get_query, src, src_conn, src_get_query, src_time_frame)

        logger.info(f"Started Destination Stage")
        try:
            start_time = time.time()
            conn = sql.connect(**dest_conn)
            dest_exist = sql.fire_query(conn, dest_check_query)
            if not dest_exist:
                sql.fire_query(conn, dest_create_query)

            status, no_of_rec = Uploader.dump_records(conn, dest_insert_query, records)
            total_time = time.time() - start_time
            logger.info(
                f"Ingestion Status: [{status}]. Total Records inserted: [{no_of_rec}]. Total Time: [{total_time}]"
            )
        except Exception as err:
            logger.error(f"Source Stage Error: {err}")

        cleaner.clean(
            conn,
            dest_exist,
            status,
            no_of_rec,
            dest,
            dest_conn,
            dest_check_query,
            dest_create_query,
            dest_insert_query,
        )
