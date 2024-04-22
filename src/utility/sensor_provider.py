from faker import Faker
from faker.providers import BaseProvider
import datetime
import random
from schemas.data import Data
from schemas.sensor_data import SensorData
from schemas.gateway import Gateway
import line_profiler

class SensorProvider(BaseProvider):
    @profile
    def data_specific(self, fake) -> Data:
        #fake = Faker()
        dr = fake.bothify(text='??# ??### #/#').upper()
        ts = fake.random_int(min=1000000000000, max=9999999999999)
        EUI = fake.uuid4()
        ack = fake.random_element(elements=(True, False))
        bat = 0
        cmd = "gw"
        gws = []
        for x in range(random.randint(1,2)):
            ts = fake.random_int(min=1000000000000, max=9999999999999)
            ant = 1
            lat = float(fake.numerify(text='56.######'))
            lon = float(fake.numerify(text='8.########'))
            snr = -6
            rssi = (fake.random_int(min=70, max=110))*-1
            time = datetime.datetime.now()
            tmms = 50000
            gweui = fake.bothify(text='####??????##?##?').upper()
            new_gws = Gateway(ts, ant, lat, lon, snr, rssi, time, tmms, gweui)
            gws.append(new_gws)
        toa = 0
        data = fake.lexify(text='????????????????????????????????????????????????????????????????')
        fcnt = fake.random_int(min=1000, max=9999)
        freq = fake.random_int(min=100000000, max=999999999)
        port = 2
        seqno = fake.random_int(min=1000000, max=9999999)
        return Data(dr, ts, EUI, ack, bat, cmd, gws, toa, data, fcnt, freq, port, seqno)
    @profile
    def data(self, fake) -> SensorData:
        #fake = Faker()
        id = fake.uuid4()
        gateway_type = 'test'
        time = datetime.datetime.now()
        was_sensor_data_for_sensor_id = fake.uuid4()
        data_version = "sensor_v2_0_0"
        return SensorData(id, gateway_type, time, was_sensor_data_for_sensor_id, data_version)