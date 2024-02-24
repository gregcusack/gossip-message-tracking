from Validators import Validators
from StakeBucket import StakeBucket, NUM_PUSH_ACTIVE_SET_ENTRIES
from GossipCrdsSample import GossipCrdsSample, GossipCrdsSampleBySignature

class MessageDistribution:
    def __init__(self, influx):
        print("msg distribution")
        self.influx = influx
        self.nodes_per_bucket =  [0 for _ in range(NUM_PUSH_ACTIVE_SET_ENTRIES)] # 25 buckets
        """
        maps: bucket -> dist o
        """
        self.distributions_by_bucket = {}
        self.get_validators()

    def get_validators(self):
        self.validators = Validators('data/validator-stakes.json', 'data/validator-gossip.json')
        self.validators.load_gossip()
        self.validators.load_stakes()
        self.validators.merge_stake_and_gossip()
        self.validators.sort(ascending=False)

        self.stake_map = self.validators.get_validator_stake_map(8)

        print("All rpc/validators: " + str(len(self.validators.get_all())))

    def get_nodes_per_bucket(self):
        for _, stake in self.stake_map.items():
            bucket = StakeBucket.get_stake_bucket(int(stake))
            self.nodes_per_bucket[bucket] += 1
        print(self.nodes_per_bucket)

    def run(self):
        self.get_nodes_per_bucket()
        origins_to_run = ['CW9C7HBw']
        for origin in origins_to_run:
            self.calculate_message_distribution(origin)
            origin_bucket = StakeBucket.get_stake_bucket(int(self.stake_map[origin]))
            if origin_bucket not in self.distributions_by_bucket:
                self.distributions_by_bucket[origin_bucket] = [0.0 for _ in range(NUM_PUSH_ACTIVE_SET_ENTRIES)]

    """
    B24
    A has 10 origins
    C has 7 origins.
    - B24 has 17 messages
    - Say B22 (has 30 nodes.) 17*30 would be max number. but say we get 17*10 would be 10/30 nodes
    - receives each message on average
    So divide. count up each time a node in a bucket receives a message. So then B22 sum would be
    like 170 messages received from the 17 sent. Then we store in B22: 170/17 (the average)
    so we store the average number of nodes in B22 received per message

    B10
    D has 100 origins
    E has 400 origins
    F has 700 origins
    - B10 has 1200 messages


    """
    def calculate_message_distribution(self, origin):
        origin_data = self.query_origin_data(origin)
        origin_data_by_signature = self.group_origin_data_by_signature(origin_data)
        if len(origin_data_by_signature) == 0:
            # TODO: think we can just return nothing??
            print("TODO: need to handle if there is not data from this signature")
        bucket_set = {} # bucket -> # messages received count
        number_messages = len(origin_data_by_signature.keys())
        for signature, data in origin_data_by_signature.items():
            visited_nodes = set() # use to make sure we do not double count nodes
            host_id_bucket = StakeBucket.get_stake_bucket(int(self.stake_map[data.host_id]))
            source_bucket = StakeBucket.get_stake_bucket(int(self.stake_map[data.source]))
            if data.host_id not in visited_nodes:
                visited_nodes.add(data.host_id)
            if data.source not in visited_nodes:
                visited_nodes.add(data.source)

            """
            # ok need to figure out how to count all the nodes we have visited
            but need to ensure we do not double count since we are looking at
            host_id and source.
            for each signature, count all the nodes the signature gets to
            but we are counting buckets. 
            """

            if host_id_bucket not in bucket_set:
                bucket_set[host_id_bucket] = 0.0
            if source_bucket not in bucket_set:
                bucket_set[source_bucket] = 0.0
            bucket_set[host_id_bucket] += 1
            bucket_set[source_bucket] += 1
            print(signature, data)


    """
    origin_data: type ResultSet (influx query result)
    returns a dict: signature -> GossipCrdsSample
    """
    def group_origin_data_by_signature(self, origin_data):
        data_by_signature = GossipCrdsSampleBySignature()
        return data_by_signature.process_data(origin_data)

    def query_origin_data(self, origin):
        return self.influx.get_data_by_single_origin(origin)

