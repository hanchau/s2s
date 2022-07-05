from mysql.connector import connect, Error


class SQLConnector:
    def __init__(self, logger):
        self.logger = logger

    def connect(self,
        host="localhost", user="anuj", pwd="ANUJ@123@mysql", db="agrim", port=3306
    ):
        try:
            conn = connect(host=host, user=user, password=pwd, database=db, port=port)
            self.logger.info(f"Connection established with: [{host}|{port}|{user}|{db}]")
            return conn
        except Exception as err:
            self.logger.error(f"Error Connecting: {err}")

    def fire_query(self, conn, query, rec_in_json=False):
        try:
            with conn.cursor(dictionary=rec_in_json) as cursor:
                cursor.execute(query.replace("\n", " "))
                result = cursor.fetchall()
            del cursor
            self.logger.info(f"Fired Query Successfully: \n----\n{query}\n---\n")
            return result
        except Exception as err:
            self.logger.error(f"Error in firing Query: {err}")


