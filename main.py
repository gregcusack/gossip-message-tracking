from GossipQueryInflux import GossipQueryInflux
import sys
from Graph import Graph


if __name__ == "__main__":
    hash = sys.argv[1] # could be origin or signature

    influx = GossipQueryInflux()
    result = influx.get_data_by_signature(hash)
    # print(result)
    data = influx.convert_query_result_to_tuple(result)

    graph = Graph()
    graph.build(data)
    graph.cycle_exists()
    graph.draw()
    graph.save_plot()


