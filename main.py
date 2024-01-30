from GossipQueryInflux import GossipQueryInflux
import sys
from Graph import Graph
import json
import pandas as pd
from Validators import Validators
from Stats import Stats

CHARS_TO_KEEP = 8

if __name__ == "__main__":
    percentage = float(sys.argv[2])
    validators = Validators('data/validator-stakes.json')
    validators.load()
    validators.sort(ascending=False)

    trimmed_validators = validators.get_validators_by_cummulative_stake_percentage(percentage)

    print("Number of validators: " + str(len(trimmed_validators)))

    influx = GossipQueryInflux()

    if sys.argv[1] == "graph":
        hash = sys.argv[3] # could be origin or signature

        result = influx.get_data_by_signature(hash)
        data = influx.transform_query_results(result)

        graph = Graph()
        graph.build(data, color=True, nodes_to_color=validators.get_host_ids_first_n_chars(trimmed_validators, CHARS_TO_KEEP).tolist())
        graph.cycle_exists()
        graph.draw()
        graph.configure_legend(hash, percentage)
        graph.save_plot()

    elif sys.argv[1] == "ingress":
        num_origins = int(sys.argv[3])
        origins = validators.get_top_n_highest_staked_validators(num_origins)
        origins = validators.get_host_ids_first_n_chars(origins, CHARS_TO_KEEP).tolist()

        result = influx.get_data_by_multiple_origins(origins)
        data = influx.transform_query_results(result)

        stats = Stats(data)
        validator_stake_map = validators.get_validator_stake_map(CHARS_TO_KEEP)
        stats.populate_source_metrics_per_host(validator_stake_map, CHARS_TO_KEEP)
        # stats.print_host_to_source_mapping()

        stats.calculate_median_source_stake_per_host_id()

        sorted_list_by_host_id_stake = stats.sort_by_host_id_stake(validator_stake_map)
        stats.plot_host_id_vs_median_stake(sorted_list_by_host_id_stake, num_origins)

        # print(stats.sort_by_source_median_stake(validator_stake_map))





