
LAMPORTS_PER_SOL = 1000000000
NUM_PUSH_ACTIVE_SET_ENTRIES = 25

class StakeBucket:
    def __init__(self, origin):
        self.origin = origin


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

        if stake_in_sol == 0:
            bucket = 0
        else:
            bucket = stake_in_sol.bit_length()

        print(f"stake, bucket: ({stake_in_sol}, {bucket})")

        return min(bucket, NUM_PUSH_ACTIVE_SET_ENTRIES - 1)

