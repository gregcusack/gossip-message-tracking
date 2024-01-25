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
    "voteAccountPubkey"
]

class Validators:
    def __init__(self, path_to_stake_file):
        self.path = path_to_stake_file

    def load(self):
        raw_data = json.load(open(self.path))
        stakes = pd.DataFrame(raw_data["validators"])
        self.validators = stakes.drop(stake_columns_to_drop, axis=1)

    def sort(self, ascending):
        self.validators = self.validators.sort_values(by='activatedStake', ascending=ascending)
        self.validators = self.validators.reset_index(drop=True)
        self.validators.rename(columns = {'identityPubkey':'host_id'}, inplace = True)

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
                self.validators = self.validators.iloc[:index+1]

    def get_host_ids(self):
        return self.validators['host_id'].unique()

    def count(self):
        return len(self.validators.index)

    def print_validators(self):
        print(self.validators)