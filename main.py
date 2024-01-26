from GossipQueryInflux import GossipQueryInflux
import sys
from Graph import Graph
import json
import pandas as pd
from Stake import Validators


if __name__ == "__main__":
    hash = sys.argv[1] # could be origin or signature
    percentage = 100
    if len(sys.argv) > 2:
        percentage = float(sys.argv[2])

    validators = Validators('data/validator-stakes.json')
    validators.load()
    validators.sort(ascending=False)

    validators.get_validators_by_cummulative_stake_percentage(percentage)

    print("Number of validators: " + str(validators.count()))

    print(validators.get_host_ids().tolist())
    # print(type(validators.get_host_ids()))
    influx = GossipQueryInflux()
    result = influx.get_data_by_signature_and_host_ids(hash, validators.get_host_ids().tolist())
    print(result)

    """
    TODO: take these host_ids. and then pass them into a new query function
    We only want to query based on specific signature and if
     1) from==host_ids
        - would get the next hop beyond the staked nodes we are interested in. aka who the nodes
          we are interested in are sending to
     2) host_id==host_ids
        - this gets who is sending messages to the nodes are interested in.

    Node of interest is one within the top "percentage" of nodes by stake
     Note: We need to think how this is going to look. it will be pretty disconnected right?
     if there is a node we don't care about in the path between the origin and the node of interest
     we'll see <rand> -> <NODE> and we will see <NODE> -> <rand>
     <origin> -> <rand1> -> <NODE>
        - we won't know who rand1 got their data from (unless origin is of interest. which it could be)
     <origin> -> <rand1> -> <rand2> -> <NODE>
        - we will see <origin> -> <rand1> connection (if origin is node of interest) and <rand2> -> <NODE> connection
        - but we will not see <rand1> -> <rand2> so there will be a disconnect and will be unknown
    """

    # influx = GossipQueryInflux()
    # result = influx.get_data_by_signature(hash)
    # print(result)
    data = influx.convert_query_result_to_tuple(result)




    # print(validators)

    graph = Graph()
    graph.build(data)
    graph.cycle_exists()
    graph.draw()
    graph.save_plot()


