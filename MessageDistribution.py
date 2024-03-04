from Validators import Validators
from StakeBucket import StakeBucket, NUM_PUSH_ACTIVE_SET_ENTRIES
from GossipCrdsSample import GossipCrdsSample, GossipCrdsSampleBySignature
import matplotlib.pyplot as plt

class MessageDistribution:
    def __init__(self, influx):
        self.influx = influx
        self.nodes_per_bucket =  [0 for _ in range(NUM_PUSH_ACTIVE_SET_ENTRIES)] # 25 buckets
        """
        maps: bucket -> dist o
        """
        self.raw_messages_received_by_bucket = {}
        self.get_validators()

        self.messages_created_per_bucket =  [0 for _ in range(NUM_PUSH_ACTIVE_SET_ENTRIES)] # 25 buckets

        self.normalized_distribution_by_bucket = {}

    def get_validators(self):
        self.validators = Validators('data/validator-stakes-2.json', 'data/validator-gossip-2.json')
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

    def run(self):
        self.get_nodes_per_bucket()
        # origins_to_run = ['CW9C7HBw', 'q9XWcZ7T', 'Fd7btgyS']#, 'UPSCQNqd', 'DWvDTSh3']
        # origins_to_run = self.validators.get_all_staked_host_ids()
        origins_to_run = self.validators.get_all_entries_after_host_id('EUavyHnV')
        # print(origins_to_run)
        # print(self.validators.get_validator_stake_map(8))
        for count, origin in enumerate(origins_to_run):
            print(count)
            # if origin not in self.stake_map:
            #     print(f"Warning: origin: {origin} not in stake map. not counting...")
            #     continue
            origin_bucket = StakeBucket.get_stake_bucket(int(self.stake_map[origin]))
            if origin_bucket not in self.raw_messages_received_by_bucket:
                self.raw_messages_received_by_bucket[origin_bucket] = [0.0 for _ in range(NUM_PUSH_ACTIVE_SET_ENTRIES)]
            self.calculate_message_distribution(origin, origin_bucket)

            if count + 1 < len(origins_to_run):
                # if origins_to_run[count + 1] not in self.stake_map:
                #     print(f"Warning: origin: {origin} not in stake map. not counting...")
                #     continue
                next_bucket = StakeBucket.get_stake_bucket(int(self.stake_map[origins_to_run[count + 1]]))
                # if next bucket is not same as this bucket, we are done with validators for this stake bucket
                # so lets plot the origin_bucket
                if next_bucket != origin_bucket:
                    self.normalize(origin_bucket)
                    self.plot(origin_bucket)
            elif count + 1 == len(origins_to_run):
                print("at end")
                self.normalize(origin_bucket)
                self.plot(origin_bucket)


        print("############# END DATA ##############")
        print(f"nodes per bucket: {self.nodes_per_bucket}")
        print(f"messages created per bucket: {self.messages_created_per_bucket}")
        # for origin_bucket, buckets in self.raw_messages_received_by_bucket.items():
        #     print(f"origin bucket: {origin_bucket}, msg_received per bucket: {buckets}")

        #     # Normalize by Number of Nodes in Each Bucket
        #     normalized_by_nodes = [msg / nodes if nodes > 0 else 0 for msg, nodes in zip(self.raw_messages_received_by_bucket[origin_bucket], self.nodes_per_bucket)]
        #     # print(f"normalized by nodes: {normalized_by_nodes}")

        #     # Normalize by Number of Messages Created by Origin Bucket
        #     total_messages_from_origin = self.messages_created_per_bucket[origin_bucket]
        #     normalized_distribution = [count / total_messages_from_origin for count in normalized_by_nodes]
        #     print(f"norm distribution: {normalized_distribution}")
        #     self.normalized_distribution_by_bucket[origin_bucket] = normalized_distribution

        #     self.plot(origin_bucket)

    def normalize(self, origin_bucket):
        print(f"origin bucket: {origin_bucket}, msg_received per bucket: {self.raw_messages_received_by_bucket[origin_bucket]}")

        # Normalize by Number of Nodes in Each Bucket
        normalized_by_nodes = [msg / nodes if nodes > 0 else 0 for msg, nodes in zip(self.raw_messages_received_by_bucket[origin_bucket], self.nodes_per_bucket)]
        # print(f"normalized by nodes: {normalized_by_nodes}")

        # Normalize by Number of Messages Created by Origin Bucket
        total_messages_from_origin = self.messages_created_per_bucket[origin_bucket]
        normalized_distribution = [count / total_messages_from_origin for count in normalized_by_nodes]
        print(f"norm distribution: {normalized_distribution}")
        self.normalized_distribution_by_bucket[origin_bucket] = normalized_distribution

    def plot(self, origin_bucket):
        buckets = list(range(NUM_PUSH_ACTIVE_SET_ENTRIES))
        # Creating the plot
        plt.figure(figsize=(10, 6))  # Adjust the figure size as needed
        plt.bar(buckets, self.normalized_distribution_by_bucket[origin_bucket], color='skyblue')  # Plot bars

        # Adding titles and labels
        plt.title(f'Normalized Message Distribution from Bucket {origin_bucket}')
        plt.xlabel('Receiver Stake Bucket')
        plt.ylabel('Normalized Messages Received per Node')
        plt.xticks(buckets)  # Ensure all bucket labels are shown

        # Show plot
        # plt.show()
        plt.savefig(f"plots/receiver_bucket_dist_for_origin_bucket_{origin_bucket}")


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
    def calculate_message_distribution(self, origin, origin_bucket):
        origin_data = self.query_origin_data(origin)
        origin_data_by_signature = self.group_origin_data_by_signature(origin_data)
        if len(origin_data_by_signature) == 0:
            # TODO: think we can just return nothing??
            print("TODO: need to handle if there is not data from this signature")
        received_messages_by_bucket = [0.0 for _ in range(NUM_PUSH_ACTIVE_SET_ENTRIES)] # bucket -> # messages received count

        num_messages = len(origin_data_by_signature.keys())
        self.update_messages_per_bucket(num_messages, origin_bucket)
        for _, data in origin_data_by_signature.items():
            for point in data:
                visited_nodes = set() # use to make sure we do not double count nodes
                host_id_bucket = self.get_stake_bucket(point.host_id)
                source_bucket = self.get_stake_bucket(point.source)

                if point.host_id not in visited_nodes:
                    # haven't visited node for this signature
                    visited_nodes.add(point.host_id)
                    received_messages_by_bucket[host_id_bucket] += 1
                    self.raw_messages_received_by_bucket[origin_bucket][host_id_bucket] += 1

                if point.source not in visited_nodes:
                    # haven't visited node for this signature
                    visited_nodes.add(point.source)
                    received_messages_by_bucket[source_bucket] += 1
                    self.raw_messages_received_by_bucket[origin_bucket][source_bucket] += 1

                """
                # ok need to figure out how to count all the nodes we have visited
                but need to ensure we do not double count since we are looking at
                host_id and source.
                for each signature, count all the nodes the signature gets to
                but we are counting buckets.
                """
        # self.distributions_by_bucket[origin_bucket] = received_messages_by_bucket
        print(f"origin: {origin}, bucket: {origin_bucket}, received_messages_by_bucket: {received_messages_by_bucket}")

    def get_stake_bucket(self, host_id):
        try:
            return StakeBucket.get_stake_bucket(int(self.stake_map[host_id]))
        except KeyError as e:
            # print(f"WARN: {e} not in stake_map. assuming stake is 0")
            return 0

    """
    origin_data: type ResultSet (influx query result)
    returns a dict: signature -> GossipCrdsSample
    """
    def group_origin_data_by_signature(self, origin_data):
        data_by_signature = GossipCrdsSampleBySignature()
        return data_by_signature.process_data(origin_data)

    def query_origin_data(self, origin):
        return self.influx.get_data_by_single_origin(origin)

    """
    track number of messages created per bucket
    this will be used to normalize the number of messages per bucket
    since not all buckets will create the same number of messages
    """
    def update_messages_per_bucket(self, num_messages, origin_bucket):
        if origin_bucket > 24 or origin_bucket < 0:
            print(f"ERROR: origin_bucket is incorrectly sized: {origin_bucket}")
        self.messages_created_per_bucket[origin_bucket] += num_messages


