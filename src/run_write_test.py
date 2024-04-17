import psycopg2
import configparser
from faker import Faker
from faker.providers import BaseProvider
import uuid
import json
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
    print(fake.random_letters(length=12))

def test_sensor_write_speed(amount: int, connection):
    print("Not Implemented")

def get_insert_query(amount: int):
    queries = []

    for x in range(amount):
        print("waps")

    print("Not Implemented")

class SensorData:
    def __init__(self, id, gateway_type, data, time, was_sensor_data_for_sensor_id, data_version):
        self.id = id
        self.gateway_type = gateway_type
        self.data = data
        self.time = time
        self.was_sensor_data_for_sensor_id = was_sensor_data_for_sensor_id
        self.data_version = data_version

class Data:
    def __init__(self, dr, ts, EUI, ack, bat, cmd, gws, toa, data, fcnt, freq, port, seqno):
        self.dr = dr
        self.ts = ts
        self.EUI = EUI
        self.ack = ack
        self.bat = bat
        self.cmd = cmd
        self.gws = gws
        self.toa = toa
        self.data = data
        self.fcnt = fcnt
        self.freq = freq
        self.port = port
        self.seqno = seqno

class MyProvider(BaseProvider):
    def data(self) -> Data:
        #https://faker.readthedocs.io/en/master/providers/baseprovider.html#faker.providers.BaseProvider
        fake = Faker()
        dr = fake.lexify(text='????????????')
        ts = fake.random_int(min=1000000000000, max=9999999999999)
        EUI = uuid.uuid4()
        ack = fake.random_element(elements=(True, False))
        bat = "something"
        cmd = "something"
        gws = "something"
        toa = "something"
        data = ["something", "something2"]
        fcnt = "something"
        freq = "something"
        port = "something"
        seqno = "something"


def generate_sensor_data(amount: int):
    fake = Faker()
    sensor_data = []

    for x in range(amount):
        id = uuid.uuid4()
        gateway_type = "test"
        data = []


run()