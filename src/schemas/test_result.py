class TestResult:
    def __init__(self, timestamp, total_time, average_time, is_pool, run_id):
        self.timestamp = timestamp
        self.total_time = total_time
        self.average_time = average_time
        self.is_pool = is_pool
        self.run_id = run_id