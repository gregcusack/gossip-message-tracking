from dataclasses import dataclass

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

@dataclass
class CoverageBySignature:
    """
    Represents coverage metrics by signature.

    Attributes:
        signature (str): signature of message
        staked_length (int): Number of staked nodes that received the message
        unstaked_length (int): Number of unstaked nodes that received the message
        staked_coverage (float): staked_nodes that received the message / all staked nodes in network
        overall_coverage (float): all nodes that received the message / all nodes in network
        fully_connected_coverage (float): all nodes directly connected to origin / all nodes in the network
        possible_peak_connected_coverage (float):
            (all nodes directly connected to origin +
            non_metric_reporting_nodes that have an in-degree of 0 +
            all of (2)'s descendants) /
            divided by all nodes in the network
        staked_fully_connected_coverage (float): same as 'fully_connected_coverage' but only staked nodes
        staked_possible_peak_connected_coverage (float): same as 'possible_peak_connected_coverage' but only staked nodes
        ...
    """
    signature: str
    staked_length: int
    unstaked_length: int
    staked_coverage: float
    overall_coverage: float
    fully_connected_coverage: float
    possible_peak_connected_coverage: float
    staked_fully_connected_coverage: float
    staked_possible_peak_connected_coverage: float

    def __str__(self):
        return (f"CoverageBySignature:\n"
                f" signature={self.signature}\n"
                f" staked_length={self.staked_length}\n"
                f" unstaked_length={self.unstaked_length}\n"
                f" staked_coverage={self.staked_coverage:.3f}\n"
                f" overall_coverage={self.overall_coverage:.3f}\n"
                f" fully_connected_coverage={self.fully_connected_coverage:.3f}\n"
                f" possible_peak_connected_coverage={self.possible_peak_connected_coverage:.3f}\n"
                f" staked_fully_connected_coverage={self.staked_fully_connected_coverage:.3f}\n"
                f" staked_possible_peak_connected_coverage={self.staked_possible_peak_connected_coverage:.3f}")




class CoverageStatsByOrigin:
    def __init__(self, origin, origin_rank):
        self.origin = origin
        self.origin_rank = origin_rank
        self.stats_by_signature = {}

    def insert(self, coverage_data_by_signature: CoverageBySignature):
        self.stats_by_signature[coverage_data_by_signature.signature] = coverage_data_by_signature

    def get(self, signature):
        return self.stats_by_signature[signature]
