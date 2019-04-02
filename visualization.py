import matplotlib.pyplot as plt
import networkx as nx
import dbHelper

plt.rcParams["figure.figsize"] = (15,10)

db = dbHelper.New_dbHelper()
nodes = db.get_links()


G = nx.Graph()
G.add_edges_from(nodes)

nx.draw_spring(G,node_size=[1 for i in range(len(G.nodes))], width = 0.5)



plt.show()


