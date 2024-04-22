class SensorData:
    def __init__(self, id, gateway_type, time, was_sensor_data_for_sensor_id, data_version):
        self.id = id
        self.gateway_type = gateway_type
        self.time = time
        self.data = None
        self.was_sensor_data_for_sensor_id = was_sensor_data_for_sensor_id
        self.data_version = data_version