import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pygraphviz as pgv
from networkx.drawing.nx_agraph import graphviz_layout
from GossipCrdsSample import GossipCrdsSample


class NodeStyle:
    def __init__(self, color, size):
        self.color = color
        self.size = size

class Graph:
    def __init__(self):
        self.G = nx.DiGraph()
        self.node_styles = {}
        self.colored_count = 0
        self.nodes_to_color = 0

    """
    data is of type GossipCrdsSample
    """
    def build(self, data, color=False, nodes_to_color=None, special_style=NodeStyle('red', 700), default_style=NodeStyle('blue', 300)): #special_color='red', default_color='blue', special_size=700, default_size=300):
        for sample in data:
            if sample.source == None or sample.host_id == None:
                print("ERROR: source or host is None!: " + str(sample.source) + ", " + str(sample.host_id))
                continue
            self.G.add_edge(sample.source, sample.host_id[:8])

            if color and nodes_to_color is not None:
                self.nodes_to_color = nodes_to_color
                # Assign colors if coloring is enabled
                for node in [sample.source, sample.host_id[:8]]:
                    self.node_styles[node] = special_style if node in nodes_to_color else default_style

        if color:
            self.colored_count = sum(
                (node_style.color == special_style.color and node_style.size == special_style.size)
                for node_style in self.node_styles.values()
            )


            print("Total Nodes colored: {} out of {} top N% of nodes by stake".format(self.colored_count, len(nodes_to_color)))



    def draw(self):
        # plt.figure(figsize=(20,10))
        plt.figure(figsize=(200, 120)) # can use 200, 120 to get a little more spacing
        pos = graphviz_layout(self.G, prog='neato')  # This one also good
        # pos = graphviz_layout(self.G, prog='dot')  # THIS ONE IS GREAT
        # pos = nx.nx_agraph.graphviz_layout(self.G, prog='dot')

        colors = [self.node_styles.get(node, NodeStyle('blue', 100)).color for node in self.G.nodes()]
        sizes = [self.node_styles.get(node, NodeStyle('blue', 100)).size for node in self.G.nodes()]

        nx.draw(self.G, pos=pos, node_color=colors, node_size=sizes, with_labels=True)

    """
    percentage: top percentage of staked nodes
    """
    def configure_legend(self, signature, percentage):
        # Create a legend
        red_patch = mpatches.Patch(color='red', label=f"Top {percentage}% of staked Nodes \n ({self.colored_count} found out of {len(self.nodes_to_color)} possible)")
        blue_patch = mpatches.Patch(color='blue', label='All other Nodes')
        plt.legend(handles=[red_patch, blue_patch], fontsize=100)
        # plt.title("HEY", fontsize=30, loc="left")
        plt.title(f"{signature} message propagation. Highlighting top {percentage}% of nodes by stake", fontsize=100, loc="center", backgroundcolor='green', color='orange')
        # plt.tight_layout()

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