"""
Median Coverage
Mean Coverage

bucket distribution of nodes Rx message

"""


class MessageSignatureSets:
    def __init__(self):
        self.staked = set() # staked nodes
        self.unstaked = set() # unstaked nodes only
        self.unknown = set() # in query but not in gossip or staked nodes

    def staked_len(self):
        return len(self.staked)

    def unstaked_len(self):
        return len(self.unstaked)

    def length_all(self):
        return len(self.staked) + len(self.unstaked)

    def add(self, host_id, stake_map):
        if host_id in stake_map:
            if stake_map.get(host_id) == 0:
                # print("unstaked")
                # if host_id in self.unstaked:
                    # print("already in")
                self.unstaked.add(host_id)
            else:
                # print("staked")
                # if host_id in self.staked:
                #     print("already in")
                self.staked.add(host_id)
        else:
            self.unknown.add(host_id)

class CoverageStats:
    def __init__(self):
        print("cov stats")
        self.sets = MessageSignatureSets()



