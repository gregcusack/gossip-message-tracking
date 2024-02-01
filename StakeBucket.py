import csv
import numpy as np

LAMPORTS_PER_SOL = 1000000000
NUM_PUSH_ACTIVE_SET_ENTRIES = 25

class BucketContents:
    def __init__(self):
        self.count = 0
        self.cummulative_stake = 0
        self.median_stake = 0
        self.stakes = []

    def update(self, stake):
        self.count += 1
        self.cummulative_stake += stake
        self.stakes.append(stake)
        # self.calculate_median_stake()

    def calculate_median_stake(self):
        # Extract stakes from the tuples
        stakes = np.array([stake for stake in self.stakes])

        # Use numpy's median function
        self.median_stake = np.median(stakes)

class StakeBucket:
    def __init__(self, total_stake):
        self.total_stake = total_stake
        self.buckets = [BucketContents() for _ in range(NUM_PUSH_ACTIVE_SET_ENTRIES)]
        # self.buckets = [BucketContents()] * NUM_PUSH_ACTIVE_SET_ENTRIES


    """
    // Maps stake to bucket index.
    fn get_stake_bucket(stake: Option&u64) -> usize {
        let stake = stake.copied().unwrap_or_default() / LAMPORTS_PER_SOL;
        let bucket = u64::BITS - stake.leading_zeros();
        (bucket as usize).min(NUM_PUSH_ACTIVE_SET_ENTRIES - 1)
    """
    def get_stake_bucket(self, stake):
        if stake is None:
            stake = 0

        stake_in_sol = stake // LAMPORTS_PER_SOL
        bucket = stake_in_sol.bit_length()

        return min(bucket, NUM_PUSH_ACTIVE_SET_ENTRIES - 1)

    def set_stake_buckets(self, validator_stake_map):
        for node, stake in validator_stake_map.items():
            bucket = self.get_stake_bucket(int(stake))
            # print(bucket, stake)
            self.buckets[bucket].update(stake // LAMPORTS_PER_SOL)
            # print(self.buckets[bucket].count)
            # self.buckets[bucket] += 1

    def print_buckets(self):
        for i, contents in enumerate(self.buckets):
            # print(f"bucket: {i}, count: {contents.count}")
            print(f"bucket: {i}, count: {contents.count}, stake: {contents.cummulative_stake}")

    def get_stake_cummulative_stake_percentage_per_bucket(self):
        for i, contents in enumerate(self.buckets):
            # print(contents.cummulative_stake, total_stake)
            fractional_stake = contents.cummulative_stake / self.total_stake * 100
            print(f"bucket: {i}, count: {contents.count}, fraction of total stake: {fractional_stake}%")


    def bucket_contents_to_file(self):
        all_rows = []
        for i, contents in enumerate(self.buckets):
            contents.calculate_median_stake()
            fractional_stake = contents.cummulative_stake / self.total_stake * 100
            row = [i, contents.count, contents.cummulative_stake, contents.median_stake, f"{fractional_stake:.8f}%"]
            all_rows.append(row)


        filename = 'data/stake_per_bucket.csv'
        with open(filename, mode='w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['bucket', '# nodes', 'sum of node stakes (Sol)', 'median stake (Sol)', 'fraction of total stake'])
            for row in all_rows:
                csvwriter.writerow(row)


# origin -> buckets