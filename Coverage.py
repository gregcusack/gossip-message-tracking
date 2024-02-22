
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
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

class Coverage:
    def __init__(self):
        self.influx = GossipQueryInflux()
        self.coverage_stats_by_origin = {} # origin -> CoverageStatsByOrigin mapping

    def run_data_collection(self):
        self.get_validators()
        non_reporting_staked_host_ids = self.get_metric_non_reporting_nodes()

        # origins_to_run = self.validators.get_top_n_host_ids_by_stake(5)
        # origins_to_run = self.validators.get_all_staked_host_ids()
        origins_to_run = self.validators.get_all_entries_after_host_id('GafF2qoG')
        print(origins_to_run)
        # print(f"Running for {len(origins_to_run)} origins")
        # origins_to_run = ['GafF2qoG']

        for origin in origins_to_run:
            print(f"Currently running origin: {origin}")
            mean_coverage, median_coverage = self.run_for_origin(origin, non_reporting_staked_host_ids)
            self.write_aggregate_coverage_stat_to_file(origin, mean_coverage, median_coverage)

    def run_for_origin(self, origin, non_reporting_staked_host_ids):
        origin_data = self.query_origin_data(origin)
        origin_data_by_signature = self.group_origin_data_by_signature(origin_data)
        # check if data returned from influx is empty
        if len(origin_data_by_signature) == 0:
            return self.return_empty_coverage_stats_by_signature()
        origin_rank = self.validators.get_rank_by_origin(origin)
        self.coverage_stats_by_origin[origin] = CoverageStatsByOrigin(origin, origin_rank)

        # iterate through all of the signatures for a specific origin
        for signature in origin_data_by_signature.keys():
            print(f"------------ signature: {signature} ------------")
            data = origin_data_by_signature[signature]
            # find connectivity of nodes and descendants
            self.run_graph_analysis(origin, data, non_reporting_staked_host_ids)
            self.calculate_coverage_by_origin_and_signature(origin, signature, data)

        print(f"all signatures from origin: {self.coverage_stats_by_origin[origin].get_signatures()}")
        mean_coverage, median_coverage = self.coverage_stats_by_origin[origin].get_aggregate_coverage_stats()

        print(f"Mean Stats for origin {origin}: {mean_coverage}")
        print(f"Median Stats for origin {origin}: {median_coverage}")

        return mean_coverage, median_coverage


    """
    data of type [GossipCrdsSample]
    """
    def calculate_coverage_by_origin_and_signature(self, origin, signature, data):
        gossip_data_by_signature_sets = MessageSignatureSets()
        for entry in data:
            gossip_data_by_signature_sets.add(entry.source, self.stake_map)
            gossip_data_by_signature_sets.add(entry.host_id, self.stake_map)

        possible_connected_node_signature_sets = MessageSignatureSets()
        for host_id in self.descendants_from_origin:
            possible_connected_node_signature_sets.add(host_id, self.stake_map)
        for host_id in self.possibly_connected_nodes_set:
            possible_connected_node_signature_sets.add(host_id, self.stake_map)

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

    """
    origin_data: type ResultSet (influx query result)
    returns a dict: signature -> GossipCrdsSample
    """
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
        non_reporting_staked_host_ids = ReportMetrics.identify_non_reporting_staked_hosts(self.validators.get_host_ids_staked_validators(), host_set)
        return non_reporting_staked_host_ids

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
        print(f"Non-metric-reporting host_ids without incoming edges length: {len(host_ids_without_incoming_edges)}")
        # print(f"Non-metric-reporting host_ids without incoming edges values: {host_ids_without_incoming_edges}")
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

    def write_aggregate_coverage_stat_to_file(self, origin, mean_coverage, median_coverage):
         # Convert the CoverageBySignature instances to dictionaries, excluding 'signature'
        mean_dict = {k: v for k, v in vars(mean_coverage).items() if k != 'signature'}
        median_dict = {k: v for k, v in vars(median_coverage).items() if k != 'signature'}

        # Add the origin information to the dictionaries
        mean_dict['origin'] = origin
        median_dict['origin'] = origin

        # Create a DataFrame from the dictionaries
        df = pd.DataFrame([mean_dict, median_dict])
        df.index = ["Mean", "Median"]
        df.index.name = "AggregateType"

        # Reset the index to turn AggregateType into a column
        df.reset_index(inplace=True)

        # Reorder columns to have 'origin' as the first column, followed by 'AggregateType'
        # The rest of the column order remains as is
        column_order = ['origin', 'AggregateType'] + [col for col in df.columns if col not in ['origin', 'AggregateType']]
        df = df[column_order]

        # Apply formatting: limit float columns to 3 decimal places by converting to strings
        for col in df.select_dtypes(include=['float']).columns:
            df[col] = df[col].apply(lambda x: f"{x:.3f}")

        file_path = "coverage-v2.csv"

        # Check if the file exists to determine if we need to write headers
        file_exists = os.path.isfile(file_path)
        df.to_csv(file_path, mode='a', header=not file_exists, index=False)


    def write_aggregate_coverage_stats_to_file(self, origins, mean_coverages, median_coverages):
        # Initialize an empty DataFrame
        final_df = pd.DataFrame()

        # Loop through the arrays
        for origin, mean, median in zip(origins, mean_coverages, median_coverages):
            # Extracting attributes from CoverageBySignature instances
            data = {
                'origin': [origin, origin],
                'AggregateType': ['Mean', 'Median'],
                'staked_length': [mean.staked_length, median.staked_length],
                'unstaked_length': [mean.unstaked_length, median.unstaked_length],
                'staked_coverage': [f"{mean.staked_coverage:.3f}", f"{median.staked_coverage:.3f}"],
                'overall_coverage': [f"{mean.overall_coverage:.3f}", f"{median.overall_coverage:.3f}"],
                'fully_connected_coverage': [f"{mean.fully_connected_coverage:.3f}", f"{median.fully_connected_coverage:.3f}"],
                'possible_peak_connected_coverage': [f"{mean.possible_peak_connected_coverage:.3f}", f"{median.possible_peak_connected_coverage:.3f}"],
                'staked_fully_connected_coverage': [f"{mean.staked_fully_connected_coverage:.3f}", f"{median.staked_fully_connected_coverage:.3f}"],
                'staked_possible_peak_connected_coverage': [f"{mean.staked_possible_peak_connected_coverage:.3f}", f"{median.staked_possible_peak_connected_coverage:.3f}"]
            }
            # Temporary DataFrame for the current set of values
            temp_df = pd.DataFrame(data)

            # Append the temporary DataFrame to the final DataFrame
            final_df = pd.concat([final_df, temp_df], ignore_index=True)

        final_df.to_csv("coverage.csv", index=False)

    def return_empty_coverage_stats_by_signature(self):
            mean_coverage = CoverageBySignature("zero data", np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan)
            median_coverage = CoverageBySignature("zero data", np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan)
            return mean_coverage, median_coverage

    def plot_all(self, filepath):
        df = pd.read_csv(filepath)
        mean_df = df[df['AggregateType'] == 'Mean']
        median_df = df[df['AggregateType'] == 'Median']

        self.plot_stat(mean_df, 'Mean')
        self.plot_stat(median_df, 'Median')

    def plot_stat(self, df, stat_type):
        plt.figure(figsize=(200, 60))
        for column in ['staked_coverage', 'overall_coverage', 'fully_connected_coverage',
                   'possible_peak_connected_coverage', 'staked_fully_connected_coverage',
                   'staked_possible_peak_connected_coverage']:

            # plot all origins, but if origin has NaN data, just leave that y value blank
            origins = df['origin'].unique()
            y_values = []
            for origin in origins:
                # Extract the row for the current origin and metric; handle NaN by not plotting a point
                value = df[df['origin'] == origin][column].values
                if len(value) > 0 and not np.isnan(value[0]):
                    y_values.append(value[0])
                else:
                    y_values.append(np.nan)  # Keep NaN in the list to avoid plotting but maintain the x-axis position

            plt.plot(origins, y_values, marker='o', label=column, linestyle='-')


        plt.tick_params(axis='y', labelsize=80)
        plt.grid(axis='y')
        plt.ylim(0, 1) # y limit to 1

        y_ticks = np.arange(0, 1.05, 0.05)  # Grid lines at every 0.05
        y_labels = [f"{x:.1f}" if x in np.arange(0, 1.1, 0.2) else "" for x in y_ticks]  # Label only at 0, 0.2, ..., 1.0
        plt.yticks(ticks=y_ticks, labels=y_labels)

        plt.title(f'{stat_type} Coverage by Origin', fontsize=125)
        plt.xlabel('Origin', fontsize=100)
        plt.ylabel('Coverage', fontsize=100)
        plt.xticks(rotation=90)  # Rotate the x-axis labels for better readability
        plt.legend(fontsize=25)
        plt.margins(x=0.005, tight=True)
        plt.savefig(f'plots/{stat_type}_coverage_by_origin.png')
        plt.show()