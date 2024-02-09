import json
import pandas as pd

stake_columns_to_drop = [
    "lastVote",
    "rootSlot",
    "credits",
    "epochCredits",
    "delinquent",
    "skipRate",
    "commission",
    "voteAccountPubkey",
    "version"
]

gossip_columns_to_drop = [
    "ipAddress",
    "gossipPort",
    "featureSet",
    "rpcHost",
    "tpuPort",
    "version",
    "pubsubHost"
]

class Validators:
    def __init__(self, path_to_stake_file, path_to_gossip_file):
        self.path = path_to_stake_file
        self.path_gossip = path_to_gossip_file

    def load_stakes(self):
        raw_data = json.load(open(self.path))
        stakes = pd.DataFrame(raw_data["validators"])
        self.validator_stakes = stakes.drop(stake_columns_to_drop, axis=1)
        # print(self.validator_stakes)

    def load_gossip(self):
        raw_data = json.load(open(self.path_gossip))
        gossip_validators = pd.DataFrame(raw_data)
        self.gossip_validators = gossip_validators.drop(gossip_columns_to_drop, axis=1)
        # print(self.gossip_validators)

    def merge_stake_and_gossip(self):
        # Merge the dataframes on 'identityPubkey', doing a left merge to keep all rows from 'gossip'
        self.validators = pd.merge(self.gossip_validators, self.validator_stakes, on='identityPubkey', how='left')

        # Replace NaN values in 'activatedStake' with 0
        self.validators['activatedStake'] = self.validators['activatedStake'].fillna(0)

        # print(self.validators)

    def sort(self, ascending):
        self.validators = self.validators.sort_values(by='activatedStake', ascending=ascending)
        self.validators = self.validators.reset_index(drop=True)
        self.validators.rename(columns = {'identityPubkey':'host_id'}, inplace = True)

    def sort_staked(self, ascending):
        self.validator_stakes = self.validator_stakes.sort_values(by='activatedStake', ascending=ascending)
        self.validator_stakes = self.validator_stakes.reset_index(drop=True)
        self.validator_stakes.rename(columns = {'identityPubkey':'host_id'}, inplace = True)


    def total_stake(self):
        return self.validators['activatedStake'].sum()

    def target_stake(self, percentage):
        return self.total_stake() * (percentage / 100)

    """
    Basically a cdf
    We have a df of sorted validators. We take up to and including 'percentage' of the validators by stake.
    """
    def get_validators_by_cummulative_stake_percentage(self, percentage):
        sum_stake = 0
        target_stake = self.target_stake(percentage)
        for index, row in self.validators.iterrows():
            sum_stake += row['activatedStake']
            if sum_stake >= target_stake:
                return self.validators.iloc[:index+1]

    def get_host_ids(self):
        return self.validators['host_id'].unique()

    def get_host_ids_staked_validators(self):
        return self.validator_stakes['host_id'].unique()

    def trim_host_ids(self):
        self.validators['host_id'] = self.validators['host_id'].str[:8]

    def trim_host_ids_staked(self):
        self.validator_stakes['host_id'] = self.validator_stakes['host_id'].str[:8]

    def get_host_ids_first_n_chars(self, trimmed_validators, n):
        if n < 1:
            print("ERROR: too few characters requests. defaulting to 8 chars")
            n = 8
        return trimmed_validators['host_id'].str[:n].unique()

    def count(self):
        return len(self.validators.index)

    def print_validators(self):
        print(self.validators)

    """
    Assumes self.validators is sorted already (descending)
    n: number of validators to return
    """
    def get_top_n_highest_staked_validators(self, n):
        return self.validators.head(n)

    def get_validators_in_range(self, start, end):
        return self.validators.iloc[start:end + 1]

    """
    Assumes self.validators is sorted already (descending)
    returns sorted validators by stake
    """
    def get_all(self):
        return self.validators

    def get_all_staked(self):
        return self.validator_stakes


    def get_validator_stake_map(self, n):
        # Truncate 'host_id' to first n characters and create a new column for it
        self.validators['truncated_host_id'] = self.validators['host_id'].apply(lambda x: x[:n])

        # Use the truncated 'host_id' as the index and convert the 'activatedStake' column to a dictionary
        stake_map = self.validators.set_index('truncated_host_id')['activatedStake'].to_dict()

        # Optionally, you might want to clean up by dropping the temporary column if not needed
        self.validators.drop(columns=['truncated_host_id'], inplace=True)

        return stake_map

    # def get_validator_stake_map(self, n):
    #     trimmed_host_ids = self.get_host_ids_first_n_chars(self.validators, n)
    #     return trimmed_host_ids.set_index('host_id')['activatedStake'].to_dict()
