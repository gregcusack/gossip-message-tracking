from GossipCrdsSample import GossipCrdsSample
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter

LAMPORTS_PER_SOL = 1000000000.0

class SourceStakeMetrics:
    # origin | origin stake | node | node stake | # of msgs | median of senders stake
    def __init__(self, sources=[], median=0, mode="", count=0):
        # list of (host_ids, stake) tuples that sent us messages
        # we want this to be a list since multiple entries for each source matters
        # when calculating median stake
        self.sources = sources
        print(self.sources)
        print(type(self.sources))
        self.median = median
        self.mode = mode # most common source host_id
        self.count = count # number of times mode host_id was seen

    """
    source: this is the "from" in influx (aka who sent us the messaage)
    stake: stake of the "source" node
    """
    def insert_source(self, source, stake):
        self.sources.append((source, stake))

    def calculate_median_stake(self):
        # Extract stakes from the tuples
        stakes = np.array([stake for _, stake in self.sources])

        self.median = np.median(stakes)

    def calculate_mode_host_ids(self):
        host_ids = [host_id for host_id, _ in self.sources]
        # print(len(host_ids))
        # print(host_ids)

        counter = Counter(host_ids)
        self.mode, self.count = counter.most_common(1)[0]
        # print(self.mode, self.count)

    def __str__(self):
        list_str = ', '.join(str(source) for source in self.sources)
        return f"Sources and Stakes: [{list_str}]"

class Stats:
    """
    data: list of GossipCrdsSamples
    """
    def __init__(self, data):
        self.data = data
        self.host_to_source_stake = {}

    """
    validator_stakes: dictionary: host_id -> stake
    n: first n chars of validator pubkey
    """
    def populate_source_metrics_per_host(self, validator_stakes, n):
        for sample in self.data:
            # if sample.source not in validator_stakes. sample.source is not staked
            # so we set it to 0
            stake = validator_stakes.get(sample.source, 0)
            self.host_to_source_stake.setdefault(sample.host_id[:n], SourceStakeMetrics()).insert_source(sample.source, stake)

    def print_host_to_source_mapping(self):
        for host_id, sources in self.host_to_source_stake.items():
            print(host_id)
            print(sources)

    def calculate_median_source_stake_per_host_id(self):
        for _, sources in self.host_to_source_stake.items():
            sources.calculate_median_stake()

    def calculate_mode_source_stake_per_host_id(self):
        for host_id, sources in self.host_to_source_stake.items():
            sources.calculate_mode_host_ids()

    """
    validator_stakes. dict: host_id -> stake where host_id is first n chars
    1) Will return a list of host_id, median_source_stake, host_stake sorted
    by host_stake in descending order.
    2) host_ids will only contain host_ids that report metrics
    """
    def sort_by_host_id_stake(self, validator_stakes):
        combined_data = [
            (host_id, SourceStakeMetrics(median=sources.median, mode=sources.mode, count=sources.count), validator_stakes.get(host_id, 0))
            for host_id, sources in self.host_to_source_stake.items()
        ]

        # Sort the combined data by host_stake in descending order
        sorted_data = sorted(combined_data, key=lambda x: x[2], reverse=True)

        # Format the output to include host_id, SourceStakeMetrics, host_stake mapping
        sorted_list = [(item[0], item[1], item[2]) for item in sorted_data]

        return sorted_list

    """
    validator_stakes. dict: host_id -> stake where host_id is first n chars
    1) Will return a list of host_id, median_source_stake, host_stake sorted
    by median_stake in descending order.
    2) host_ids will only contain host_ids that report metrics
    """
    def sort_by_source_median_stake(self, validator_stakes):
        combined_data = [
            (host_id, sources.median, validator_stakes.get(host_id, 0))
            for host_id, sources in self.host_to_source_stake.items()
        ]

        # Sort the combined data by host_stake in descending order
        sorted_data = sorted(combined_data, key=lambda x: x[1], reverse=True)

        # Format the output to include host_id, median_stake, host_stake mapping
        sorted_output = [(item[0], item[1], item[2]) for item in sorted_data]

        return sorted_output

    """
    sorted_list_by_host_id_stake: list of tuples sorted by host_id stake
    num_origins: number of origins we considering. just for plot title
    """
    def plot_host_id_vs_median_stake(self, sorted_list_by_host_id_stake, num_origins):
        # Filter the list to include only host_ids with stake > 0
        filtered_list = [(host_id, source_stake_metrics.median) for host_id, source_stake_metrics, host_stake in sorted_list_by_host_id_stake if host_stake > 0]
        # Extract host_ids and median_stakes
        host_ids = [item[0] for item in filtered_list]
        median_stakes = [item[1] / LAMPORTS_PER_SOL for item in filtered_list] # convert lamports to sol

        # Use indices on the x-axis for simplicity
        x = range(len(host_ids))

        # Create plot
        plt.figure(figsize=(200, 60))
        plt.plot(x, median_stakes, marker='x', linewidth=3)  # Plot median_stake vs. x (indices)
        plt.ylim(0, 250000) # y limit to 300k sol
        plt.margins(x=0.005, tight=True)

        plt.xlabel('Host IDs (sorted by host stake - descending)', fontsize=100)
        plt.ylabel('Median Stake (Sol)', fontsize=100)
        plt.title('Median Ingress Stake (Sol) by Host ID.\nOnly considering messages from the top ' + str(num_origins) + ' origins by stake', fontsize=125)

        plt.xticks(x, host_ids, rotation='vertical', fontsize=8)
        plt.tick_params(axis='y', labelsize=80)

        plt.tight_layout()
        plt.savefig('host_id_vs_median_stake_top_' + str(num_origins) + '_origins.png')

    @staticmethod
    def print_sorted_list_by_host_id_stake(sorted_list_by_host_id_stake):
        print("host_id | mode_source | mode_source_count | median_stake | host_stake")
        for host_id, stake_metrics, host_stake in sorted_list_by_host_id_stake:
            print(host_id, stake_metrics.mode, stake_metrics.count, stake_metrics.median, host_stake / LAMPORTS_PER_SOL)