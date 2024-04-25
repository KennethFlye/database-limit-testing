import psycopg2
import configparser
from faker import Faker
import jsonpickle
from utility.sensor_provider import SensorProvider
from schemas.test_result import TestResult
import time
from multiprocessing import Pool
import os
import pika
import redis
import uuid
from itertools import repeat

def run(amount_write, amount_pools, run_id, script_id):
    config = configparser.ConfigParser()

    config.read(r'src/config/settings/config.ini')

    db_database = config.get("Database", "db_database")
    db_user = config.get("Database", "db_user")
    db_password = config.get("Database", "db_password")
    db_host = config.get("Database", "db_server_host")
    db_port = config.get("Database", "db_port")

    conn = psycopg2.connect(dbname=db_database, user=db_user, password=db_password, host=db_host, port=db_port)

    test_sensor_write_speed_pool_file(amount_write, amount_pools, run_id, script_id, conn)

def test_sensor_write_speed_pool_file(amount_write, amount_pools, run_id, script_id, conn):
    start_time_get_queries = time.perf_counter()
    filename = "queries.sql"
    queries = get_queries_from_file(filename)
    end_time_get_queries = time.perf_counter()
    print(f"\nTime for queries using file: {end_time_get_queries-start_time_get_queries}")

    start_time = time.perf_counter()

    split_query_list = split_list(queries, amount_pools)

    with Pool(amount_pools) as pool:
        pool.starmap(insert_queries, zip(split_query_list, repeat(run_id), repeat(script_id)))

    end_time = time.perf_counter()
    time_taken = end_time - start_time
    average_time = time_taken/amount_write
    print(f"\nTotal Time: {time_taken}\nAverage: {time_taken/amount_write}")

    filename = "time_taken_overall.log"

    time_format = "%d/%m/%Y, %H:%M:%S"
    
    time_now = time.strftime(time_format, time.localtime())

    with open(filename, "a") as file:
        file.write(f"Timestamp: {time_now}\nTotal Time: {time_taken}\nAverage per Write: {average_time} \n\n")

    with open("time_taken_pools.log", "a") as file:
        file.write("-----------------\n\n")

    test_result_overall = TestResult(time_now, time_taken, average_time, False, run_id)
    r = setup_redis_connection()

    r.rpush(f'time_taken_overall:{run_id}', jsonpickle.encode(test_result_overall))

def split_list(queries, amount_pools):
    split_query_list = []
    list_size = len(queries)

    for x in range(amount_pools):
        new_list_size = int(list_size/amount_pools)
        start_slice = x * new_list_size
        end_slice = start_slice + new_list_size
        split_query_list.append(queries[start_slice:end_slice])

    return split_query_list

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

def insert_queries(queries: list, run_id, script_id):
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
        cursor.execute(query)
        conn.commit()
        end_time = time.perf_counter()
    
    end_time_insert = time.perf_counter()
    total_time_queries = end_time_insert - start_time_insert
    average_time_queries = total_time_queries/len(queries)

    print(f"Insert Time: {total_time_queries} & Average: {total_time_queries/len(queries)}, Connection Time: {time_connection}")
    filename = "time_taken_pools.log"

    time_format = "%d/%m/%Y, %H:%M:%S"

    time_now = time.strftime(time_format, time.localtime())

    with open(filename, "a") as file:
        file.write(f"Timestamp: {time_now}\nInsert Time: {total_time_queries}\nAverage per Write: {average_time_queries} \n\n")

    test_result_pool = TestResult(time_now, total_time_queries, average_time_queries, True, run_id)
    test_result_pool.script_id = script_id
    r = setup_redis_connection()

    r.rpush(f'time_taken_pools:{run_id}', jsonpickle.encode(test_result_pool))


def get_insert_queries_for_pool(amount: int) -> list:
    queries = []

    fake = Faker()
    fake.add_provider(SensorProvider)

    for x in range(amount):
        data_object = fake.data(fake)
        data_object.data = fake.data_specific(fake)

        query = f'INSERT INTO \"SensorDatas\" (\"Id\", \"GatewayType\", \"Data\", \"Time\", \"WasSensorDataForSensorId\", \"DataVersion\") VALUES (\'{data_object.id}\', \'{data_object.gateway_type}\', \'{jsonpickle.encode(data_object.data, unpicklable=False)}\', \'{data_object.time}\', \'{data_object.was_sensor_data_for_sensor_id}\', \'{data_object.data_version}\')'

        queries.append(query)
    
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

def setup_mq_connection():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='write', exchange_type='fanout')

    result_write = channel.queue_declare(queue='', exclusive=True)
    queue_name_write = result_write.method.queue

    channel.queue_bind(exchange='write', queue=queue_name_write)


    channel.exchange_declare(exchange='query', exchange_type='fanout')

    result_query = channel.queue_declare(queue='', exclusive=True)
    queue_name_query = result_query.method.queue

    channel.queue_bind(exchange='query', queue=queue_name_query)


    channel.exchange_declare(exchange='clear', exchange_type='fanout')

    result_clear = channel.queue_declare(queue='', exclusive=True)
    queue_name_clear = result_clear.method.queue

    channel.queue_bind(exchange='clear', queue=queue_name_clear)

    return channel, queue_name_query, queue_name_write, queue_name_clear

def clear_sensor_table():
    config = configparser.ConfigParser()

    config.read(r'src/config/settings/config.ini')

    db_database = config.get("Database", "db_database")
    db_user = config.get("Database", "db_user")
    db_password = config.get("Database", "db_password")
    db_host = config.get("Database", "db_server_host")
    db_port = config.get("Database", "db_port")

    conn = psycopg2.connect(dbname=db_database, user=db_user, password=db_password, host=db_host, port=db_port)

    cursor = conn.cursor()

    cursor.execute("DELETE FROM \"SensorDatas\" WHERE \"GatewayType\"=\'test\';")
    conn.commit()

def setup_redis_connection():
    return redis.Redis(host='localhost', port=6379, decode_responses=True)

if __name__ == '__main__':
    amount_write = int(os.environ.get("DATA_POINTS", 50000))
    amount_pools = int(os.environ.get("POOLS", 10))

    channel, queue_name_query, queue_name_write, queue_name_clear = setup_mq_connection()
    r = setup_redis_connection()

    def query_callback(ch, method, properties, body):
        queries_to_file(amount_write, amount_pools)
        ch.basic_ack(delivery_tag = method.delivery_tag)
        print("Done Creating Queries")

    def write_callback(ch, method, properties, body):
        script_id = str(uuid.uuid4())
        run(amount_write, amount_pools, body.decode("utf-8"), script_id)
        ch.basic_ack(delivery_tag = method.delivery_tag)
        print("Done Inserting to Database")

    def clear_callback(ch, method, properties, body):
        clear_sensor_table()
        ch.basic_ack(delivery_tag = method.delivery_tag)
        print("Done Clearing Sensor Table")

    channel.basic_consume(
        queue=queue_name_query, on_message_callback=query_callback
    )

    channel.basic_consume(
        queue=queue_name_write, on_message_callback=write_callback
    )

    channel.basic_consume(
        queue=queue_name_clear, on_message_callback=clear_callback
    )

    print("\nWaiting for messages")
    channel.start_consuming()
