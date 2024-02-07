from GossipCrdsSample import GossipCrdsSample
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter
import csv
from StakeBucket import StakeBucket

LAMPORTS_PER_SOL = 1000000000.0

class SourceStakeMetrics:
    def __init__(self, sources=None, median_source_stake=0, num_ingress_messages=0, mode_source_host_ids="", count=0):
        # list of host_ids that sent us messages
        # we want this to be a list since multiple entries for each source matters
        # when calculating median stake
        if sources is None:
            sources = []
        self.sources = sources
        self.median_source_stake = median_source_stake
        self.num_ingress_messages = num_ingress_messages
        self.mode_source_host_ids = mode_source_host_ids
        self.count = count

    """
    source: this is the "from" in influx (aka who sent us the messaage)
    stake: stake of the "source" node
    """
    def insert_source(self, source, stake):
        self.sources.append((source, stake))
        self.num_ingress_messages += 1

    def calculate_median_stake(self):
        # Extract stakes from the tuples
        stakes = np.array([stake for _, stake in self.sources])
        self.median_source_stake = np.median(stakes)

    def calculate_mode_source(self):
        host_ids = np.array([host_id for host_id, _ in self.sources])
        counter = Counter(host_ids)
        self.mode_source_host_ids, self.count = counter.most_common(1)[0]  # Gets the most common element and its count

    def __str__(self):
        sources_str = ', '.join([f"({host_id}, {stake})" for host_id, stake in self.sources])
        return (f"Sources: [{sources_str}]\n"
                f"Median Source Stake: {self.median_source_stake}\n"
                f"Number of Ingress Messages: {self.num_ingress_messages}\n"
                f"Mode Source Host IDs: {self.mode_source_host_ids}\n"
                f"Count: {self.count}")

class Stats:
    """
    data: list of GossipCrdsSamples
    """
    def __init__(self, data):
        self.data = data
        self.host_to_source_stake = {}
        self.origin_to_host_to_metrics_map = {}

    """
    validator_stakes: dictionary: host_id -> stake
    n: first n chars of validator pubkey
    """
    def populate_source_metrics_per_host(self, validator_stakes, n):
        for sample in self.data:
            # Ensure host_to_metrics dict exists for this origin
            host_to_metrics = self.origin_to_host_to_metrics_map.setdefault(sample.origin, {})

            # Fetch or create the SourceStakeMetrics instance for the truncated host_id
            trimmed_host_id = sample.host_id[:n]
            if trimmed_host_id not in host_to_metrics:
                host_to_metrics[trimmed_host_id] = SourceStakeMetrics()

            # Fetch stake from validator_stakes, defaulting to 0 if not found
            stake = validator_stakes.get(sample.source, 0)

            # Insert source and stake into the SourceStakeMetrics instance
            host_to_metrics[trimmed_host_id].insert_source(sample.source, stake)

            # Ensure the updated host_to_metrics dict is assigned back to the origin
            # This step is actually redundant as the dictionaries are mutable and already updated in place
            self.origin_to_host_to_metrics_map[sample.origin] = host_to_metrics

    def print_origin_to_host_to_source_mapping(self):
        for origin, host_ids in self.origin_to_host_to_metrics_map.items():
            print(f"############ ORIGIN: {origin} #############")
            for host_id, metrics in host_ids.items():
                print(host_id)
                print(metrics)

    def calculate_source_stake_per_host_id_per_origin_metrics(self):
        for _, host_ids in self.origin_to_host_to_metrics_map.items():
            for _, metrics in host_ids.items():
                metrics.calculate_median_stake()
                metrics.calculate_mode_source()

    def sort_origin_to_host_to_metrics_mapping(self, validator_stake_map):
        all_rows = []
        for origin, host_to_metrics in self.origin_to_host_to_metrics_map.items():
            origin_stake = validator_stake_map.get(origin, 0)
            for host_id, metrics in host_to_metrics.items():
                host_id_stake = validator_stake_map.get(host_id, 0)
                if host_id_stake == 0:
                    median_over_host_stake = 0
                else:
                    median_over_host_stake = (metrics.median_source_stake / host_id_stake)

                # print(median_over_host_stake, metrics.median_source_stake, host_id_stake)
                row = [
                    origin,
                    origin_stake / LAMPORTS_PER_SOL,
                    host_id,
                    host_id_stake / LAMPORTS_PER_SOL,
                    metrics.num_ingress_messages,
                    metrics.median_source_stake / LAMPORTS_PER_SOL,
                    median_over_host_stake, #median as fraction of host_id stake
                    metrics.mode_source_host_ids,
                    metrics.count
                ]
                all_rows.append(row)

        return sorted(all_rows, key=lambda x: (x[1], x[3]), reverse=True)

    def write_origin_to_host_to_metrics_to_csv(self, sorted_rows, filename):
        with open(filename, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['origin', 'origin stake', 'host_id', 'host_id stake', '# of msgs', 'median of senders stake', 'median/host_id stake (Sol)', 'mode source', 'count mode'])
            for row in sorted_rows:
                # Ensure numerical values are rounded/formatted to two decimal points as needed when writing rows
                formatted_row = [row[0], f"{row[1]:.2f}", row[2], f"{row[3]:.2f}", row[4], f"{row[5]:.2f}", row[6], row[7], row[8]]
                csvwriter.writerow(formatted_row)

    def plot_median_ingress_stake_for_origin(self, sorted_rows, origin):
        # Filter rows for the specified origin and where host_id stake is not 0
        origin_rows = [row for row in sorted_rows if row[0] == origin and row[3] > 0]

        # Check if there's data to plot after filtering
        if not origin_rows:
            print(f"No data to plot for origin: {origin} with non-zero host_id stake.")
            return

        # Extract host_ids and median stakes for plotting
        host_ids = [row[2] for row in origin_rows]
        median_stakes = [row[5] for row in origin_rows]

        # Plotting
        x = range(len(host_ids))

        # Create plot
        plt.figure(figsize=(200, 60))
        plt.plot(x, median_stakes, marker='x', linewidth=3)  # Plot median_stake vs. x (indices)
        plt.ylim(0, 250000) # y limit to 300k sol
        plt.margins(x=0.005, tight=True)

        plt.xlabel('Host IDs (sorted by host stake - descending)', fontsize=100)
        plt.ylabel('Median Stake (Sol)', fontsize=100)
        plt.title('Median Ingress Stake (Sol) by Host ID. Origin: ' + origin, fontsize=125)

        plt.xticks(x, host_ids, rotation='vertical', fontsize=8)
        plt.tick_params(axis='y', labelsize=80)

        plt.tight_layout()
        plt.savefig('plots/host_id_vs_median_stake_for_origin_' + origin + '.png')

    def plot_median_stake_over_host_stake_for_origin(self, sorted_rows, origin, stake_rank):
        # Filter rows for the specified origin and where host_id stake is not 0
        origin_rows = [row for row in sorted_rows if row[0] == origin and row[3] > 0]

        # Check if there's data to plot after filtering
        if not origin_rows:
            print(f"No data to plot for origin: {origin} with non-zero host_id stake.")
            return

        # Extract host_ids and median stakes for plotting
        host_ids = [row[2] for row in origin_rows]
        stakes = [row[6] for row in origin_rows]

        stake_buckets = [StakeBucket.get_stake_bucket(int(row[3] * LAMPORTS_PER_SOL)) for row in origin_rows]
        bucket_changes = [i+1 for i in range(len(stake_buckets)-1) if stake_buckets[i] != stake_buckets[i+1]]

        # Plotting
        x = range(len(host_ids))

        # Create plot
        plt.figure(figsize=(200, 60))
        plt.plot(x, stakes, marker='x', linewidth=3)  # Plot median_stake vs. x (indices)
        # plt.ylim(0, 1) # y limit to 300k sol
        plt.yscale('log', base=2)
        plt.axhline(y=1, color='red', linestyle='--', linewidth=5)
        plt.margins(x=0.005, tight=True)
        for idx in bucket_changes:
            plt.axvline(x=idx, color='orange', linestyle='--', linewidth=5)

            if idx < len(host_ids) - 1:
                # Place text slightly to the left of the vertical line
                plt.text(x=idx + 0.5, y=max(stakes) / 2, s=f'B{stake_buckets[idx]}', color='orange', rotation=90, verticalalignment='center', fontsize=50)

        plt.xlabel('Host IDs (sorted by host stake - descending)', fontsize=100)
        plt.ylabel('Median Stake / Host Stake (Sol)', fontsize=100)
        plt.title('Ratio of Median Ingress Stake to Host ID Stake. Origin: ' + origin + ', Stake Rank: ' + str(stake_rank), fontsize=125)

        plt.xticks(x, host_ids, rotation='vertical', fontsize=8)
        plt.tick_params(axis='y', labelsize=80)

        plt.tight_layout()
        plt.savefig('plots/median_stake_over_host_id_stake_for_origin_stake_rank_' + str(stake_rank) + '_origin_' + origin + '.png')

    """
    validator_stakes. dict: host_id -> stake where host_id is first n chars
    1) Will return a list of host_id, median_source_stake, host_stake sorted
    by host_stake in descending order.
    2) host_ids will only contain host_ids that report metrics
    """
    def sort_by_origin_then_host_id_stake(self, validator_stakes):
        combined_data = [
            (host_id, sources.median, validator_stakes.get(host_id, 0))
            for host_id, sources in self.host_to_source_stake.items()
        ]

        # Sort the combined data by host_stake in descending order
        sorted_data = sorted(combined_data, key=lambda x: x[2], reverse=True)

        # Format the output to include host_id, median_stake, host_stake mapping
        sorted_list = [(item[0], item[1], item[2]) for item in sorted_data]

        return sorted_list

    """
    sorted_list_by_host_id_stake: list of tuples sorted by host_id stake
    num_origins: number of origins we considering. just for plot title
    """
    def plot_host_id_vs_median_stake(self, sorted_list_by_host_id_stake, num_origins):
        # Filter the list to include only host_ids with stake > 0
        filtered_list = [(host_id, median_stake) for host_id, median_stake, host_stake in sorted_list_by_host_id_stake if host_stake > 0]
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
        plt.savefig('plots/host_id_vs_median_stake_top_' + str(num_origins) + '_origins.png')
