from datetime import datetime, timedelta
import time

class RateLimit:
    def __init__(self, limit, interval):
        self.limit = limit        # 200 requests
        self.interval = interval  # 60 seconds
        self.requests = []
    
    def wait_if_needed(self):
        now = datetime.now()
        # Remove requests older than our interval
        self.requests = [req_time for req_time in self.requests 
                        if now - req_time < timedelta(seconds=self.interval)]
        
        if len(self.requests) >= self.limit:
            # Wait until oldest request expires
            sleep_time = (self.requests[0] + timedelta(seconds=self.interval) - now).total_seconds()
            if sleep_time > 0:
                time.sleep(sleep_time)
            self.requests = self.requests[1:]
        
        self.requests.append(now)