
"""
get validators by stake and sort them in descending order
for each validator
    get all data (entries) from that validator origin
    create a dict: msg_signature => Set(visited hosts)
    for entry in entries:
        check if message signature is in dict
        if not
            dict[entry.signature] = Set()
        # try to insert both the host_id and the from
        # will help ensure nodes that don't report metrics get inserted
        # won't catch all nodes though here
        dict[entry.signature].insert(entry.host_id) #try to insert
        dict[entry.signature].insert(entry.from) # try to insert


we also want to know stake distribution of coverage
- but lets do this second. focus on coverage first

"""

from Validators import Validators
from GossipQueryInflux import GossipQueryInflux
from CoverageStats import Sets, CoverageStats
import sys

class Coverage:
    def __init__(self):
        print("coverage")
        self.influx = GossipQueryInflux()

    def run(self):
        print("run")
        self.get_validators()
        print(self.validators.get_all())
        count = 0
        # loop over all host_ids in network
        for row in self.validators.get_all().itertuples():
            count += 1
            if count % 200 != 1:
                # lets do every 200
                continue
            print("------------------------")
            # starting with the highest staked validator,
            # use the row.host_id as the origin
            # query all data with an origin == row.host_id
            data = self.query_origin_data(row.host_id)
            msg_sig_to_host_dict = {}
            for entry in data:
                if entry.signature not in msg_sig_to_host_dict:
                    msg_sig_to_host_dict[entry.signature] = Sets()
                msg_sig_to_host_dict[entry.signature].add(entry.source, self.stake_map)
                msg_sig_to_host_dict[entry.signature].add(entry.host_id, self.stake_map)
                # print(entry)
            for signature, node_set in msg_sig_to_host_dict.items():
                print(f"stake rank: {count}, "
                        f"origin: {row.host_id}, "
                        f"sig: {signature}, "
                        f"stake_len: {node_set.staked_len()}, "
                        f"unstaked_len: {node_set.unstaked_len()}, "
                        f"staked_coverage: {node_set.staked_len()/len(self.validators.get_all_staked())}, "
                        f"overall_coverage: {node_set.length_all()/len(self.stake_map)}")
            # if count == 4:
            #     sys.exit(0)

    def get_validators(self):
        self.validators = Validators('data/validator-stakes-2.json', 'data/validator-gossip-2.json')
        self.validators.load_gossip()
        self.validators.load_stakes()
        self.validators.merge_stake_and_gossip()
        self.validators.sort(ascending=False)
        self.validators.trim_host_ids() # trim to 8 leading chars for host_id

        self.stake_map = self.validators.get_validator_stake_map(8)

        print("Number of validators: " + str(len(self.validators.get_all())))

    def query_origin_data(self, origin):
        result = self.influx.get_data_by_single_origin(origin)
        data = self.influx.transform_query_results(result)
        # data is a list of GossipCrdsSample() data for all entries with "origin" as the origin
        return data

