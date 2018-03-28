from pymongo import MongoClient
import arrow




def get_label_for_node(node_attr):
    return node_attr.get('image',node_attr.get('instance'))



def insert_graph(col,graph_sequence,graph_id,name):
    start_ts = graph_sequence.keys()[0] # The starting timestamp point
    col.insert_one({
        "graph_id" : graph_id,
        "creation_date" : arrow.Arrow.fromtimestamp(start_ts).datetime,
        "name" : name,
        "graph_attributes": {},
        "anomaly_level_settings": {
            "levels": [
                {
                    "name": "No-Data",
                    "value": 0,
                    "color": "black"
                },
                {
                    "name": "Normal",
                    "value": 1,
                    "color": "gray"
                },
                {
                    "name": "Warning",
                    "value": 2,
                    "color": "yellow"
                },
                {
                    "name": "Danger",
                    "value": 3,
                    "color": "red"
                }
            ]
        }
    })

def insert_nodes(col,timestamp,graph_ts):
    list_of_nodes = []
    for node_id, node_attr in graph_ts.nodes(data=True): # each node is going to be a tuple
        node_dict = {
            'node_id' : node_id,
            'label' : get_label_for_node(node_attr),
            'type': node_attr['type'],
            'attributes' : {k: v for k, v in node_attr.iteritems() if k not in ['anomalies','anomaly_level','type']},
            'anomalies' : node_attr['anomalies'],
            'anomaly_level' : node_attr['anomaly_level'],
            'timestamp' : arrow.Arrow.fromtimestamp(timestamp).datetime
        }
        list_of_nodes.append(node_dict)
    result = col.insert_many(list_of_nodes,ordered=True) # ordered is just if we want to zip the values
    return result.inserted_ids
    # return dict(zip(result.inserted_ids,graph_ts.nodes()))

def insert_edges(col,timestamp,graph_ts):
    list_of_edges = []
    for source, target, edge_attr in graph_ts.edges(data=True):
        edge_dict = {
            'edge_id' : source + '->' + target,
            'source' : source,
            'target' : target,
            'timestamp' : arrow.Arrow.fromtimestamp(timestamp).datetime,
            'attributes' : {k: v for k, v in edge_attr.iteritems() if k not in ['anomalies','anomaly_level']},
            'anomalies': edge_attr['anomalies'],
            'anomaly_level': edge_attr['anomaly_level']
        }
        list_of_edges.append(edge_dict)
    result = col.insert_many(list_of_edges,ordered=True)
    return result.inserted_ids

def insert_graph_ts(col,timestamp,nodes_ids,edges_ids,graph_id):
    col.insert_one({
        'graph_id' : graph_id,
        'timestamp' : arrow.Arrow.fromtimestamp(timestamp).datetime,
        'nodes' : nodes_ids,
        'edges' : edges_ids
    })

def insert_graph_timestamp(db,timestamp,graph_ts,graph_id):
    nodes_ids = insert_nodes(db['node'],timestamp,graph_ts)
    edges_ids = insert_edges(db['edge'],timestamp,graph_ts)
    graph_ts_ids = insert_graph_ts(db['graph.timestamp'],timestamp,nodes_ids,edges_ids,graph_id)

def mongodb_insert_graph_seq(database,graph_sequence,graph_id,name):
    """
    Insert a graph in the graph and graph.timestamps collections from a dictionary of timestamps : networkX graphs
    As parameters we can change the graph_id and name values of the graph collection
    :param database: the address of the MongoDB
    :param graph_sequence: dict {timestamp : networkX}
    :param graph_id: str
    :param name: str
    :return:
    """
    client = MongoClient(database,27017)
    db = client['graphs']
    insert_graph(db['graph'],graph_sequence,graph_id,name)
    for timestamp, graph_ts in graph_sequence.iteritems():
        insert_graph_timestamp(db,timestamp,graph_ts,graph_id)



# shortened_graph_sequence = {k: v for k, v in graph_sequence2.iteritems() if k in xrange(1509724100 - 3, 1509724100 + 2)}

