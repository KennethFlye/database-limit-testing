import psycopg2
import configparser
from faker import Faker
import uuid
import jsonpickle
from utility.sensor_provider import SensorProvider
import time
import threading
from multiprocessing import Pool
import os

def run(amount_write, amount_pools):
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

    #test_sensor_write_speed(amount_write, fake, conn)

    #test_sensor_write_speed_pool(amount_write, amount_pools, fake, conn)

    test_sensor_write_speed_pool_file(amount_write, amount_pools, conn)
    
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
    print(f"\nTotal Time: {time_taken}\nAverage: {time_taken/amount}")

def test_sensor_write_speed_pool(amount: int, amount_pools, fake, connection):
    queries = []

    start_time = time.perf_counter()
    with Pool(amount_pools) as p:
        results = p.map(get_insert_queries_for_pool, [int(amount/amount_pools)]*amount_pools)

    
    end_time = time.perf_counter()
    print(f"\nTime for queries using pool: {end_time-start_time}")

    # for result in results:
    #     queries.extend(result)

    start_time = time.perf_counter()

    with Pool(amount_pools) as pool:
        pool.map(insert_queries, results)
    #insert_queries(queries, connection)

    end_time = time.perf_counter()
    time_taken = end_time - start_time
    print(f"\nTotal Time: {time_taken}\nAverage: {time_taken/amount}")

def test_sensor_write_speed_pool_file(amount_write, amount_pools, conn):
    start_time_get_queries = time.perf_counter()
    filename = "queries.sql"
    queries = get_queries_from_file(filename)
    end_time_get_queries = time.perf_counter()
    print(f"\nTime for queries using file: {end_time_get_queries-start_time_get_queries}")

    start_time = time.perf_counter()

    with Pool(amount_pools) as pool:
        #Problem med den automatiske opsplitning af listen måske. Kan være jeg bare selv skal opdele den?
        pool.map(insert_queries, queries, amount_pools)

    end_time = time.perf_counter()
    time_taken = end_time - start_time
    print(f"\nTotal Time: {time_taken}\nAverage: {time_taken/amount_write}")

def get_queries_from_file(filename):
    try:
        queries = []
        with open(filename, "r") as file:
            for line in file:
                queries.append(line.strip())
            return queries
    except FileNotFoundError:
        print(f"File '{filename}' not found.")
        return []

def insert_queries(queries: list):
    config = configparser.ConfigParser()

    config.read(r'src/config/settings/config.ini')

    db_database = config.get("Database", "db_database")
    db_user = config.get("Database", "db_user")
    db_password = config.get("Database", "db_password")
    db_host = config.get("Database", "db_server_host")
    db_port = config.get("Database", "db_port")

    start_time = time.perf_counter()
    conn = psycopg2.connect(dbname=db_database, user=db_user, password=db_password, host=db_host, port=db_port)
    end_time = time.perf_counter()
    time_connection = end_time - start_time

    start_time_insert = time.perf_counter()
    cursor = conn.cursor()
    for query in queries:
        start_time = time.perf_counter()
        #print("Starting query")
        #cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        end_time = time.perf_counter()
        #print(f"Completed Query \nTime taken: {end_time-start_time}")
        #time_taken += end_time-start_time
    
    end_time_insert = time.perf_counter()
    total_time_queries = end_time_insert - start_time_insert

    #start_time = time.perf_counter()
    #end_time = time.perf_counter()
    print(f"Insert Time: {total_time_queries} & Average: {total_time_queries/len(queries)}, Connection Time: {time_connection}")

def get_insert_queries_for_pool(amount: int) -> list:
    queries = []

    amount_of_time_object_data = 0
    amount_of_time_object_specific = 0
    amount_of_time_queries = 0

    fake = Faker()
    fake.add_provider(SensorProvider)

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
        query = f'INSERT INTO \"SensorDatas\" (\"Id\", \"GatewayType\", \"Data\", \"Time\", \"WasSensorDataForSensorId\", \"DataVersion\") VALUES ("{data_object.id}", "{data_object.gateway_type}", "{jsonpickle.encode(data_object.data, unpicklable=False)}", "{data_object.time}", "{data_object.was_sensor_data_for_sensor_id}", "{data_object.data_version}")'
        end_time = time.perf_counter()
        amount_of_time_queries += end_time-start_time

        queries.append(query)

    # print(f"Object generating Data: Total; {amount_of_time_object_data}, Average; {amount_of_time_object_data/amount}")
    # print(f"Object generating Specific: Total; {amount_of_time_object_specific}, Average; {amount_of_time_object_specific/amount}")
    # print(f"Query generating: Total; {amount_of_time_queries}, Average; {amount_of_time_queries/amount}")
    
    return queries

def get_insert_queries(amount: int, fake) -> list:
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
        query = f"""
        INSERT INTO \"SensorDatas\" (\"Id\", \"GatewayType\", \"Data\", \"Time\", \"WasSensorDataForSensorId\", \"DataVersion\") 
        VALUES ('{data_object.id}', '{data_object.gateway_type}', '{jsonpickle.encode(data_object.data, unpicklable=False)}', '{data_object.time}', '{data_object.was_sensor_data_for_sensor_id}', '{data_object.data_version}')
        """
        end_time = time.perf_counter()
        amount_of_time_queries += end_time-start_time

        queries.append(query)

    print(f"Object generating Data: Total; {amount_of_time_object_data}, Average; {amount_of_time_object_data/amount}")
    print(f"Object generating Specific: Total; {amount_of_time_object_specific}, Average; {amount_of_time_object_specific/amount}")
    print(f"Query generating: Total; {amount_of_time_queries}, Average; {amount_of_time_queries/amount}")
    
    return queries

def queries_to_file(amount_write: int, amount_pools: int):
        queries = []

        with Pool(amount_pools) as p:
            results = p.map(get_insert_queries_for_pool, [int(amount_write/amount_pools)]*amount_pools)
            for result in results:
                queries.extend(result)
        filename = "queries.sql"

        with open(filename, "w") as file:
            for query in queries:
                file.write(query + "\n")

if __name__ == '__main__':
    amount_write = int(os.environ.get("DATA_POINTS", 50000))
    amount_pools = int(os.environ.get("POOLS", 10))
    if(int(os.environ.get("QUERY", 0)) == 1):
        queries_to_file(amount_write, amount_pools)
    else:
        run(amount_write, amount_pools)
