

class GossipCrdsSample:
    def __init__(self, timestamp, origin, source, signature, host_id):
        self.timestamp = timestamp
        self.origin = origin
        self.source = source
        self.signature = signature
        self.host_id = host_id[:8]

    def __str__(self):
        return f"ts: {self.timestamp}, origin: {self.origin}, source: {self.source}, sig: {self.signature}, id: {self.host_id}"