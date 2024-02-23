from GossipQueryInflux import GossipQueryInflux
import sys
from Graph import Graph
from Validators import Validators
from Stats import Stats
from StakeBucket import StakeBucket, LAMPORTS_PER_SOL
from Crontab import Crontab
from Coverage import Coverage
from datetime import datetime
from ReportMetrics import ReportMetrics

CHARS_TO_KEEP = 8

if __name__ == "__main__":
    influx = GossipQueryInflux()

    if sys.argv[1] == "coverage":
        coverage = Coverage()
        if sys.argv[2] == "collect":
            coverage.run_data_collection()
        elif sys.argv[2] == "plot":
            coverage.plot_all('coverage-data-12_21.csv')

    ### Run crontab loop over past 14 days
    elif sys.argv[1] == "crontab-loop":
        print("------------------------------------------------------------")
        print(f"Running Crontab Loop at: {datetime.today().strftime('%m_%d_%Y_%H_%M_%S')}")

        for i in range(14, 0, -1):
            result = influx.query_day_range(i, i-1)
            data = influx.transform_gossip_crds_sample_results(result)
            ct = Crontab()

            ct.read_df_n_days_ago(i+1)

            ct.build_df_now(data)
            print("df_today_shape from query")
            print(ct.get_df_now_size())

            if ct.df_n_days_ago_exists():
                print("df_yesterday exists")
                ct.drop_duplicates_from_current_df()

            print("df_today_shape after trim")
            print(ct.get_df_now_size())
            ct.write_df_n_days_ago_to_file(i)
            ct.reset_dfs()
            print(f"Crontab completed at: {datetime.today().strftime('%m_%d_%Y_%H_%M_%S')}")

    ### Run Crontab. designed to run twice a day
    ### Removes duplicates
    elif sys.argv[1] == "crontab":
        print(f"Running Crontab at: {datetime.today().strftime('%m_%d_%Y_%H_%M_%S')}")
        result = influx.query_last_day()
        data = influx.transform_gossip_crds_sample_results(result)

        ct = Crontab()
        ct.read_previous_df()
        ct.build_df_now(data)
        print("df_now_shape from query")
        print(ct.get_df_now_size())

        if ct.df_previous_exists():
            print("df_previous exists")
            ct.drop_duplicates_from_df_now()

        print("df_now_shape after trim")
        print(ct.get_df_now_size())
        ct.write_df_now_to_file()
        print(f"Crontab completed at: {datetime.today().strftime('%m_%d_%Y_%H_%M_%S')}")
        # sys.exit(0)

    validators = Validators('data/validator-stakes.json', 'data/validator-gossip.json')
    validators.load_gossip()
    validators.load_stakes()
    validators.merge_stake_and_gossip()
    validators.sort(ascending=False)

    if sys.argv[1] == "report-metrics":

        result = influx.query_all_push()
        # print(result)
        host_set = influx.get_host_id_tags_from_query(result)
        non_reporting_host_ids = ReportMetrics.identify_non_reporting_staked_hosts(validators.get_host_ids_staked_validators(), host_set)

    ### Plot message propagation by signature
    elif sys.argv[1] == "graph":
        percentage = float(sys.argv[2])
        hash = sys.argv[3] # could be origin or signature

        # get non_reporting nodes
        if len(sys.argv) >= 6:
            query_start_time = sys.argv[4]
            query_end_time = sys.argv[5]
            push_results = influx.query_all_push(query_start_time, query_end_time)
        else:
            push_results = influx.query_all_push()
        host_set = influx.get_host_id_tags_from_query(push_results)
        non_reporting_host_ids = ReportMetrics.identify_non_reporting_staked_hosts(validators.get_host_ids_staked_validators(), host_set)

        trimmed_validators = validators.get_validators_by_cummulative_stake_percentage(percentage)
        print(f"Number of validators that make up >= {percentage}% stake: {str(len(trimmed_validators))}")

        result = influx.get_data_by_signature(hash)
        data = influx.transform_gossip_crds_sample_results(result)

        graph = Graph()
        graph.build(data, color=True, nodes_to_color=validators.get_host_ids_first_n_chars(trimmed_validators, CHARS_TO_KEEP).tolist())
        graph.cycle_exists()
        graph.draw(non_reporting_hosts=non_reporting_host_ids)
        graph.configure_legend(hash, percentage)
        graph.save_plot()

    ### Measure median ingress stake by origin
    elif sys.argv[1] == "ingress":
        num_origins = int(sys.argv[3])
        origins = validators.get_top_n_highest_staked_validators(num_origins)
        # origins = validators.get_validators_in_range(1000, 1002)
        origins = validators.get_host_ids_first_n_chars(origins, CHARS_TO_KEEP).tolist()

        result = influx.get_data_by_multiple_origins(origins)
        data = influx.transform_gossip_crds_sample_results(result)

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

    ### Get cummulative stake per bucket
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

    ### Get bucket from stake
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
