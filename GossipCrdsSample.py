from collections import defaultdict

class GossipCrdsSample:
    def __init__(self, timestamp, origin, source, signature, host_id):
        self.timestamp = timestamp
        self.origin = origin
        self.source = source
        self.signature = signature
        self.host_id = host_id[:8]

    def __str__(self):
        return f"ts: {self.timestamp}, origin: {self.origin}, source: {self.source}, sig: {self.signature}, id: {self.host_id}"


class GossipCrdsSampleBySignature:
    def __init__(self):
        self.samples = defaultdict(list)

    def process_data(self, data):
        for point in data.get_points():
            sample = GossipCrdsSample(
                timestamp=point['time'],
                origin=point['origin'],
                source=point['from'],  # Assuming 'from' should be mapped to 'source'
                signature=point['signature'],
                host_id=point['host_id']
            )

            # Group samples by signature
            self.samples[point['signature']].append(sample)
        return self.samples