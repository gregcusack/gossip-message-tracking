import pandas as pd
import matplotlib.pyplot as plt
from Validators import Validators
import numpy as np

class DuplicatePush:
    def __init__(self, influx):
        self.influx = influx

    def get_validators(self) -> Validators:
        validators = Validators('data/tds-validator-stakes.json', 'data/tds-validator-gossip.json')
        validators.load_gossip()
        validators.load_stakes()
        validators.merge_stake_and_gossip()
        validators.sort(ascending=False)
        return validators

    def trim_validators_into_list(self, validators: Validators, top_n_stake: int) -> list:
        validators = validators.get_top_n_highest_staked_validators(top_n_stake)
        return validators['host_id'].unique().tolist()

    """
    Get validators by stake (get gossip and stake and merge)
    Get top N validators by stake
    turn validators into a list
    call modified version influx.get_data_by_multiple_origins()
        - this will filter out all validators not in top N
        - but need to modify to work with the queries we need here
    """
    def run_fraction_dup_top_n_stake(self, top_n_stake: int) -> list:
        print(f"get mean frac duplicate push for top {top_n_stake} validators by stake")
        validators = self.get_validators()
        return self.trim_validators_into_list(validators, top_n_stake)

    def run_fraction_dup(self, top_n_stake=None):
        print(f"running duplicate push analysis. top_n_stake: {top_n_stake}")
        top_host_ids_by_stake = None
        if top_n_stake:
            top_host_ids_by_stake = self.run_fraction_dup_top_n_stake(top_n_stake)
        duplicate_push_counts = self.run_dup_push_count(top_host_ids_by_stake)
        df_dup = self.process_results(duplicate_push_counts)
        if df_dup.empty:
            print("num_duplicate_push_message df is empty")
            return

        all_push_counts = self.run_all_push_success(top_host_ids_by_stake)
        df_push = self.process_results(all_push_counts)
        if df_push.empty:
            print("num_all_push_success df is empty")
            return

        all_push_fail_counts = self.run_all_push_fail(top_host_ids_by_stake)
        df_push_fail = self.process_results(all_push_fail_counts)
        if df_push_fail.empty:
            print("num_all_push_fail query df is empty")
            return

        combined_df = self.build_df(df_dup, df_push, df_push_fail)
        frac_df = self.calc_fraction_dup_push(combined_df)
        print("frac_df")
        print(frac_df)

        print("plotting...")
        self.plot(frac_df, top_n_stake)

    def build_df(self, df_dup, df_push, df_push_fail):
        combined_df = pd.concat([df_dup, df_push, df_push_fail], axis=1, join='inner')
        combined_df.columns = ['mean_num_duplicate_push_messages', 'mean_all_push_success', 'mean_all_push_fail']
        return combined_df

    def calc_fraction_dup_push(self, df):
        df['total_push'] = df['mean_all_push_success'] + df['mean_all_push_fail']
        df['fraction_dup_push'] = df['mean_num_duplicate_push_messages'] / df['total_push']
        return df

    def run_dup_push_count(self, top_n_stake=None):
        return self.influx.query_num_duplicate_push_messages(top_n_stake)

    def run_all_push_success(self, top_n_stake=None):
        return self.influx.query_all_push_success(top_n_stake)

    def run_all_push_fail(self, top_n_stake=None):
        return self.influx.query_all_push_fail(top_n_stake)

    def process_results(self, results) -> pd.DataFrame:
        points = list(results.get_points())
        if not points:
            return None
        df = pd.DataFrame(points)
        df.set_index('time', inplace=True)
        return df

    def plot(self, frac_df, top_n_stake=None):
        # Plot the ratio as a function of time
        plt.figure(figsize=(70, 30))
        plt.plot(frac_df.index, frac_df['fraction_dup_push'], marker='o', linestyle='-')
        plt.xlabel('Time', fontsize=30)
        plt.ylabel('Fraction of Duplicate Push Messages', fontsize=30)
        plt.title('Fraction of Duplicate Push Messages over all received push messages', fontsize=50)
        plt.grid(True)
        plt.ylim(0, 1) # y limit to 1
        y_ticks = np.arange(0, 1.05, 0.05)  # Grid lines at every 0.05
        plt.yticks(ticks=y_ticks)
        for i in range(0, len(frac_df.index), 24):
            plt.axvline(x=frac_df.index[i], color='black')

        plt.xticks(rotation=75)
        plt.margins(x=0.005, tight=True)
        plt.tight_layout()
        # plt.show()
        if top_n_stake:
            plt.savefig(f'plots/fraction_duplicate_push_top_{top_n_stake}_stake.png')
        else:
            plt.savefig(f'plots/fraction_duplicate_push_all.png')





    
