class Gateway:
    def __init__(self, ts, ant, lat, lon, snr, rssi, time, tmms, gweui):
        self.ts = ts
        self.ant = ant
        self.lat = lat
        self.lon = lon
        self.snr = snr
        self.rssi = rssi
        self.time = time
        self.tmms = tmms
        self.gweui = gweui