import psycopg2
import configparser
from datetime import datetime, timedelta
import time

def run():
    config = configparser.ConfigParser()

    config.read(r'src/config/settings/config.ini')

    db_database = config.get("Database", "db_database")
    db_user = config.get("Database", "db_user")
    db_password = config.get("Database", "db_password")
    db_host = config.get("Database", "db_server_host")
    db_port = config.get("Database", "db_port")

    dev_eui = "e099719d-ba6b-4557-a2bb-90e14d428153"

    conn = psycopg2.connect(dbname=db_database, user=db_user, password=db_password, host=db_host, port=db_port)

    test_sensor_read_speed(2, conn, dev_eui)


def test_sensor_read_speed(amount, connection, dev_eui):
    queries = get_queries_sensor_data(amount, dev_eui)
    
    cursor = connection.cursor()

    time_taken = 0
    for x in queries:
        start_time = time.perf_counter()
        print("Starting query")
        print(f"Query: {x}")
        cursor.execute(x)
        end_time = time.perf_counter()
        print("Completed Query")
        print(f"Time taken: {end_time-start_time}")
        print(f"Amount of Rows: {cursor.rowcount}")
        time_taken += end_time-start_time
    
    print(f"\nAverage: {time_taken/amount}")

def get_queries_sensor_data(amount, dev_eui):
    queries = []
    start_time_query = datetime.strptime("1 Jan 24", "%d %b %y")

    for x in range(amount):
        end_time_query = start_time_query+timedelta(days=7)
        query = f"SELECT * FROM \"SensorDatas\" WHERE \"WasSensorDataForSensorId\" = '{dev_eui}' AND \"Time\" > '{start_time_query.date()}' AND \"Time\" < '{end_time_query.date()}'"
        queries.append(query)
        start_time_query = end_time_query

    return queries


run()