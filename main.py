from GossipQueryInflux import GossipQueryInflux
import sys
from Graph import Graph
import json
from Validators import Validators
from Stats import Stats
from StakeBucket import StakeBucket, LAMPORTS_PER_SOL

CHARS_TO_KEEP = 8

if __name__ == "__main__":
    percentage = float(sys.argv[2])
    validators = Validators('data/validator-stakes.json', 'data/validator-gossip.json')
    validators.load_gossip()
    validators.load()
    validators.merge_stake_and_gossip()
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

        stats.calculate_source_stake_per_host_id_per_origin_metrics()

        output_csv = 'plots/origin_to_host_to_metrics_top_ ' + str(num_origins) + '_origins.csv'
        sorted_rows = stats.sort_origin_to_host_to_metrics_mapping(validator_stake_map)
        # stats.write_origin_to_host_to_metrics_to_csv(sorted_rows, output_csv)

        for stake_rank, origin in enumerate(origins):
            stats.plot_median_ingress_stake_for_origin(sorted_rows, origin)
            stats.plot_median_stake_over_host_stake_for_origin(sorted_rows, origin, stake_rank)
        # sorted_list_by_host_id_stake = stats.sort_by_origin_then_host_id_stake(validator_stake_map)
        # stats.plot_host_id_vs_median_stake(sorted_list_by_host_id_stake, num_origins)

        # print(stats.sort_by_source_median_stake(validator_stake_map))


    elif sys.argv[1] == "bucket":
        num_origins = int(sys.argv[3])
        origins = validators.get_top_n_highest_staked_validators(num_origins)
        origins = validators.get_host_ids_first_n_chars(origins, CHARS_TO_KEEP).tolist()

        validator_stake_map = validators.get_validator_stake_map(CHARS_TO_KEEP)

        total_validator_stake = validators.total_stake() // LAMPORTS_PER_SOL
        print(total_validator_stake)
        sb = StakeBucket(total_validator_stake)
        sb.set_stake_buckets(validator_stake_map)

        sb.get_stake_cummulative_stake_percentage_per_bucket()
        sb.bucket_contents_to_file()

    elif sys.argv[1] == "bucket_from_stake":
        stake = int(sys.argv[3]) # in SOL
        bucket = StakeBucket.get_stake_bucket(stake * LAMPORTS_PER_SOL)
        print(f"stake: {stake}, bucket: {bucket}")



        # print(sb.get_stake_bucket(1 * LAMPORTS_PER_SOL))
        # print(sb.get_stake_bucket(10 * LAMPORTS_PER_SOL))
        # print(sb.get_stake_bucket(100 * LAMPORTS_PER_SOL))
        # print(sb.get_stake_bucket(1000 * LAMPORTS_PER_SOL))
        # print(sb.get_stake_bucket(10000 * LAMPORTS_PER_SOL))
        # print(sb.get_stake_bucket(100000 * LAMPORTS_PER_SOL))
        # print(sb.get_stake_bucket(1000000 * LAMPORTS_PER_SOL))
        print(StakeBucket.get_stake_bucket(10000000 * LAMPORTS_PER_SOL))
        print(StakeBucket.get_stake_bucket(11809658 * LAMPORTS_PER_SOL))
        print(StakeBucket.get_stake_bucket(16700000 * LAMPORTS_PER_SOL))
