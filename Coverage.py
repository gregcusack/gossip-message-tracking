
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

we also want to find the nodes that are not reporting metrics


we also want to know stake distribution of coverage
- but lets do this second. focus on coverage first

"""

from Validators import Validators
from GossipQueryInflux import GossipQueryInflux
from CoverageStats import MessageSignatureSets, CoverageStatsByOrigin, CoverageBySignature
from ReportMetrics import ReportMetrics
from Graph import Graph
from GossipCrdsSample import GossipCrdsSampleBySignature
import sys

class Coverage:
    def __init__(self):
        self.influx = GossipQueryInflux()
        self.coverage_stats_by_origin = {} # origin -> CoverageStatsByOrigin mapping

    def run(self):
        self.get_validators()
        non_reporting_host_ids = self.get_metric_non_reporting_nodes()
        # let's just pick an origin to start
        origin = 'CW9C7HBw'
        # origin = '8n4pc4sC'
        origin_data = self.query_origin_data(origin)
        origin_data_by_signature = self.group_origin_data_by_signature(origin_data)

        signature_0 = next(iter(origin_data_by_signature.keys())) #TODO: change to all and put in loop
        print(f"signature: {signature_0}")
        data = self.query_by_signature(signature_0)

        # find connectivity of nodes and descendants
        self.run_graph_analysis(origin, data, non_reporting_host_ids)
        self.calculate_coverage_by_origin_and_signature(origin, signature_0, data)

    """
    data of type [GossipCrdsSample]
    """
    def calculate_coverage_by_origin_and_signature(self, origin, signature, data):
        origin_rank = self.validators.get_rank_by_origin(origin)
        gossip_data_by_signature_sets = MessageSignatureSets()
        for entry in data:
            gossip_data_by_signature_sets.add(entry.source, self.stake_map)
            gossip_data_by_signature_sets.add(entry.host_id, self.stake_map)

        possible_connected_node_signature_sets = MessageSignatureSets()
        for host_id in self.descendants_from_origin:
            possible_connected_node_signature_sets.add(host_id, self.stake_map)
        for host_id in self.possibly_connected_nodes_set:
            possible_connected_node_signature_sets.add(host_id, self.stake_map)

        self.coverage_stats_by_origin[origin] = CoverageStatsByOrigin(origin, origin_rank)
        coverage_stats_by_signature = CoverageBySignature(
            signature=signature,
            staked_length=gossip_data_by_signature_sets.staked_len(),
            unstaked_length=gossip_data_by_signature_sets.unstaked_len(),
            staked_coverage=gossip_data_by_signature_sets.staked_len()/len(self.validators.get_all_staked()),
            overall_coverage=gossip_data_by_signature_sets.length_all()/len(self.stake_map),
            fully_connected_coverage=len(self.descendants_from_origin)/len(self.stake_map),
            possible_peak_connected_coverage=(len(self.possibly_connected_nodes_set) + len(self.descendants_from_origin))/len(self.stake_map),
            staked_fully_connected_coverage=len(self.descendants_from_origin)/len(self.validators.get_all_staked()),
            staked_possible_peak_connected_coverage=(len(self.possibly_connected_nodes_set) + len(self.descendants_from_origin))/len(self.validators.get_all_staked()),
        )

        self.coverage_stats_by_origin[origin].insert(coverage_stats_by_signature)

        print(self.coverage_stats_by_origin[origin].get(signature))

    def calculate_coverage(self):
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
            result = self.query_origin_data(row.host_id[:8])
            data = self.transform_gossip_crds_sample_results(result)
            msg_sig_to_host_dict = {}
            for entry in data:
                if entry.signature not in msg_sig_to_host_dict:
                    msg_sig_to_host_dict[entry.signature] = MessageSignatureSets()
                msg_sig_to_host_dict[entry.signature].add(entry.source, self.stake_map)
                msg_sig_to_host_dict[entry.signature].add(entry.host_id, self.stake_map)
                # print(entry)
            for signature, node_set in msg_sig_to_host_dict.items():
                print(f"stake rank: {count}, "
                        f"origin: {row.host_id[:8]}, "
                        f"sig: {signature}, "
                        f"stake_len: {node_set.staked_len()}, "
                        f"unstaked_len: {node_set.unstaked_len()}, "
                        f"staked_coverage: {node_set.staked_len()/len(self.validators.get_all_staked())}, "
                        f"overall_coverage: {node_set.length_all()/len(self.stake_map)}")
            # if count == 4:
            #     sys.exit(0)

    def get_validators(self):
        self.validators = Validators('data/validator-stakes.json', 'data/validator-gossip.json')
        self.validators.load_gossip()
        self.validators.load_stakes()
        self.validators.merge_stake_and_gossip()
        self.validators.sort(ascending=False)
        # self.validators.trim_host_ids() # trim to 8 leading chars for host_id

        self.stake_map = self.validators.get_validator_stake_map(8)

        print("All rpc/validators: " + str(len(self.validators.get_all())))

    """
    signature: message signature
    Note: unique signature has unique origin tied to it
    """
    def query_by_signature(self, signature):
        result = self.influx.get_data_by_signature(signature)
        data = self.influx.transform_gossip_crds_sample_results(result)
        # data is a list of GossipCrdsSample() data for all entries with "origin" as the origin
        return data

    def query_origin_data(self, origin):
        return self.influx.get_data_by_single_origin(origin)

    def group_origin_data_by_signature(self, origin_data):
        data_by_signature = GossipCrdsSampleBySignature()
        return data_by_signature.process_data(origin_data)

    def transform_gossip_crds_sample_results(self, result):
        data = self.influx.transform_gossip_crds_sample_results(result)
        # data is a list of GossipCrdsSample() data for all entries with "origin" as the origin
        return data

    # we should write this to a file and keep it
    def get_metric_non_reporting_nodes(self):
        push_results = self.influx.query_all_push()
        host_set = self.influx.get_host_id_tags_from_query(push_results)
        non_reporting_host_ids = ReportMetrics.identify_non_reporting_hosts(self.validators.get_host_ids_staked_validators(), host_set)
        print(f"non metric reporting nodes: {len(non_reporting_host_ids)}")
        return non_reporting_host_ids

    """
    data is of type [CrdsGossipSample]
    """
    def build_graph(self, data):
        self.graph = Graph()
        self.graph.build(data)

    """
    make sure to drop the origin from this
    """
    def get_host_ids_without_incoming_edges(self, non_reporting_host_ids, origin):
        host_ids_without_incoming_edges = self.graph.get_nodes_without_incoming_edges(non_reporting_host_ids)
        host_ids_without_incoming_edges.discard(origin)
        print(f"Non-metric-reporting host_ids without incoming edges length: {len(host_ids_without_incoming_edges)}, values: {host_ids_without_incoming_edges}")
        return host_ids_without_incoming_edges

    def get_descendants_from_origin(self, origin):
        # get_node_descendants_from_host_ids takes in a list
        return self.get_node_descendants_from_host_ids([origin])

    """
    origin: origin node id
    host_ids_without_incoming_edges: list of all host_ids without incoming edges
    """
    def has_path_from_origin(self, origin, host_ids_without_incoming_edges):
        no_path_count = 0
        path_count = 0
        for host in host_ids_without_incoming_edges:
            if self.graph.path_exists(origin, host):
                path_count += 1
            else:
                no_path_count += 1

        # ensure that if the origin doesn't report metrics, it has a path to itself
        # but that should be the only path
        if origin in host_ids_without_incoming_edges:
            assert path_count == 1, f"Assertion failed. connected path len != 1: {path_count} != 1"
            assert no_path_count == len(host_ids_without_incoming_edges) - 1, f"Assertion failed. non connected path len != len(host_ids_without_incoming_edges) - 1: {no_path_count } != {len(host_ids_without_incoming_edges) - 11} "
        else:
            assert path_count == 0, f"Assertion failed. connected path len != 0: {path_count} != 0"
            assert no_path_count == len(host_ids_without_incoming_edges), f"Assertion failed. non connected path len != len(host_ids_without_incoming_edges): {no_path_count} != {len(host_ids_without_incoming_edges)} "

        print(f"path exists count: {path_count}")
        print(f"path doesn't exist count: {no_path_count}")

    def get_node_descendants_from_host_ids(self, host_ids_without_incoming_edges):
        reachable_from_source = set()
        for host_id in host_ids_without_incoming_edges:
            descendants = self.graph.get_node_descendants(host_id)
            reachable_from_source.update(descendants)
        return reachable_from_source

    """
    data: type [GossipCrdsSample]
    """
    def get_signatures_from_origin(self, data):
        signature_set = set()
        for entry in data:
            signature_set.add(entry.signature)
        return signature_set

    def merge_descendent_set_with_host_ids_without_incoming_edges(self, descendant_set, host_ids_without_incoming_edges):
        descendant_set.update(host_ids_without_incoming_edges)
        return descendant_set

    def run_graph_analysis(self, origin, data, non_reporting_host_ids):
        self.build_graph(data)
        host_ids_without_incoming_edges = self.get_host_ids_without_incoming_edges(non_reporting_host_ids, origin)
        # just a sanity check
        # self.has_path_from_origin(origin, host_ids_without_incoming_edges)

        # descendants from all hosts that do not report metrics and have in-degree of 0
        descendents_from_non_reporting_in_degree_zero_hosts = self.get_node_descendants_from_host_ids(host_ids_without_incoming_edges)
        print(f"descendents_from_non_reporting_in_degree_zero_hosts len: {len(descendents_from_non_reporting_in_degree_zero_hosts)}")

        # get nodes connected to origin
        self.descendants_from_origin = self.get_descendants_from_origin(origin)
        print(f"descendants_from_origin len: {len(self.descendants_from_origin)}")

        ### All non-metric reporting host_ids in-degree of 0 AND their descendants
        self.possibly_connected_nodes_set = self.merge_descendent_set_with_host_ids_without_incoming_edges(descendents_from_non_reporting_in_degree_zero_hosts, host_ids_without_incoming_edges)
        print(f"possibly_connected_nodes_set len: {len(self.possibly_connected_nodes_set)}")
