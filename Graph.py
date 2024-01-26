import networkx as nx
import matplotlib.pyplot as plt
import pygraphviz as pgv
from networkx.drawing.nx_agraph import graphviz_layout



class Graph():
    def __init__(self):
        self.G = nx.DiGraph()
        self.node_colors = {} # dict for node colors
        self.node_sizes = {}

    def build(self, data, color=False, nodes_to_color=None, special_color='red', default_color='blue'):
        for _, _, source, host_id in data:
            if source == None or host_id == None:
                print("ERROR: source or host is None!: " + str(source) + ", " + str(host_id))
                continue
            self.G.add_edge(source, host_id[:8])

            if color and nodes_to_color is not None:
                # Assign colors if coloring is enabled
                for node in [source, host_id[:8]]:
                    if node in nodes_to_color:
                        self.node_colors[node] = special_color
                    else:
                        self.node_colors[node] = default_color

        if color:
            red_count = sum(color == special_color for color in self.node_colors.values())
            print("Total Nodes colored: {} out of {} top N% of nodes by stake".format(red_count, len(nodes_to_color)))

    

    def draw(self):
        # plt.figure(figsize=(20,10))
        plt.figure(figsize=(200, 120)) # can use 200, 120 to get a little more spacing
        pos = graphviz_layout(self.G, prog='neato')  # This one also good
        # pos = graphviz_layout(self.G, prog='dot')  # THIS ONE IS GREAT
        # pos = nx.nx_agraph.graphviz_layout(self.G, prog='dot')

        colors = [self.node_colors.get(node, 'blue') for node in self.G.nodes()]  # Default to 'blue' if color not set
        nx.draw(self.G, pos=pos, node_color=colors,  with_labels=True)
        # nx.b

    def show(self):
        plt.show()

    def save_plot(self):
        plt.savefig('plot.png')

    def cycle_exists(self):
        try:
            cycle = nx.find_cycle(self.G)
            print("Cycle found: ", cycle)
            return True
        except nx.NetworkXNoCycle:
            print("No cycle found")
            return False