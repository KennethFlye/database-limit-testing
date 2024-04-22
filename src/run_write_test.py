import psycopg2
import configparser
from faker import Faker
import uuid
import jsonpickle
from utility.sensor_provider import SensorProvider
import time
from utility.caching_helper import cache_function
import line_profiler
import threading

#Overvej at bruge faker

def run():
    config = configparser.ConfigParser()

    config.read(r'src/config/settings/config.ini')

    db_database = config.get("Database", "db_database")
    db_user = config.get("Database", "db_user")
    db_password = config.get("Database", "db_password")
    db_host = config.get("Database", "db_server_host")
    db_port = config.get("Database", "db_port")

    conn = psycopg2.connect(dbname=db_database, user=db_user, password=db_password, host=db_host, port=db_port)

    fake = Faker()
    fake.add_provider(SensorProvider)

    test_sensor_write_speed(100, fake, conn)
    

def test_sensor_write_speed(amount: int, fake, connection):
    queries = get_insert_queries(amount, fake)

    time_taken = 0
    for query in queries:
        start_time = time.perf_counter()
        #print("Starting query")
        cursor = connection.cursor()
        cursor.execute(query)
        end_time = time.perf_counter()
        #print(f"Completed Query \nTime taken: {end_time-start_time}")
        time_taken += end_time-start_time
    
    connection.commit()
    print(f"\nAverage: {time_taken/amount}")

@profile
def get_insert_queries(amount: int, fake) -> list:
    # fake = Faker()
    # fake.add_provider(SensorProvider)

    queries = []

    amount_of_time_object_data = 0
    amount_of_time_object_specific = 0
    amount_of_time_queries = 0

    for x in range(amount):
        start_time = time.perf_counter()
        data_object = fake.data(fake)
        end_time = time.perf_counter()
        amount_of_time_object_data += end_time-start_time

        start_time = time.perf_counter()
        data_object.data = fake.data_specific(fake)
        end_time = time.perf_counter()
        amount_of_time_object_specific += end_time-start_time

        start_time = time.perf_counter()
        query = f"INSERT INTO \"SensorDatas\" (\"Id\", \"GatewayType\", \"Data\", \"Time\", \"WasSensorDataForSensorId\", \"DataVersion\") VALUES ('{data_object.id}', '{data_object.gateway_type}', '{jsonpickle.encode(data_object.data, unpicklable=False)}', '{data_object.time}', '{data_object.was_sensor_data_for_sensor_id}', '{data_object.data_version}')"
        end_time = time.perf_counter()
        amount_of_time_queries += end_time-start_time

        queries.append(query)

    print(f"Object generating Data: Total; {amount_of_time_object_data}, Average; {amount_of_time_object_data/amount}")
    print(f"Object generating Specific: Total; {amount_of_time_object_specific}, Average; {amount_of_time_object_specific/amount}")
    print(f"Query generating: Total; {amount_of_time_queries}, Average; {amount_of_time_queries/amount}")
    
    return queries


def generate_sensor_data(amount: int):
    fake = Faker()
    sensor_data = []

    for x in range(amount):
        id = uuid.uuid4()
        gateway_type = "test"
        data = []


run()