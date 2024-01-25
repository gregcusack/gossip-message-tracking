import networkx as nx
import matplotlib.pyplot as plt
import pygraphviz as pgv
from networkx.drawing.nx_agraph import graphviz_layout



class Graph():
    def __init__(self):
        self.G = nx.DiGraph()

    def build(self, data):
        for _, _, source, host_id in data:
            if source == None or host_id == None:
                print("ERROR: source or host is None!: " + str(source) + ", " + str(host_id))
                continue
            self.G.add_edge(source, host_id[:8])

    def draw(self):
        # plt.figure(figsize=(40,20))
        plt.figure(figsize=(100, 60)) # can use 200, 120 to get a little more spacing
        pos = graphviz_layout(self.G, prog='neato')  # This one also good
        # pos = graphviz_layout(self.G, prog='dot')  # THIS ONE IS GREAT
        # pos = nx.nx_agraph.graphviz_layout(self.G, prog='dot')

        nx.draw(self.G, pos=pos, with_labels=True)
        # nx.b

    def show(self):
        plt.show()

    def save_plot(self):
        plt.savefig('plot.png')

    def cycle_exists(self):
        try:
            cycle = nx.find_cycle(self.G)
            print("Cycle found:", cycle)
            return True
        except nx.NetworkXNoCycle:
            print("No cycle found")
            return False