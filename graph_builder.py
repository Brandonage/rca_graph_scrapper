from prometheus_df_builder import create_prometheus_df
from sysdig_df_builder import create_sysdig_df
import networkx as nx
from os import makedirs, listdir, remove
from os.path import exists, expanduser
from mongo_exporter import mongodb_insert_graph_seq
import pandas as pd
import pickle


def optional_float(f):
    try:
        return float(f)
    except ValueError:
        return f


def replace_infile(pathin, pathout, replacements):
    """
    it replaces all the occurences of a word for another one included in a dictionary
    :param pathin: the path for the input file
    :param pathout: the path where the output file will be copied
    :param replacements: a dictionary with the replacements we want to make e.g. replacements={"@namenode@":"paravance-5.grid5000.fr"}
    """
    with open(pathin) as infile, open(pathout, 'w') as outfile:
        for line in infile:
            for src, target in replacements.iteritems():
                line = line.replace(src, target)
            outfile.write(line)



def create_gephx_sequence(graph_sequence, graphs_folder):
    for timestamp, G in graph_sequence.iteritems():
        nx.write_gexf(G, "{0}{1}_classic.gexf".format(graphs_folder, timestamp))
        classic_header = "<graph defaultedgetype=\"directed\" mode=\"static\">"
        new_header = "<graph mode=\"slice\" defaultedgetype=\"directed\" timerepresentation=\"timestamp\" timestamp=\"{0}\">"
        replace_infile("{0}{1}_classic.gexf".format(graphs_folder, timestamp),
                       "{0}{1}.gexf".format(graphs_folder, timestamp),
                       {classic_header : new_header.format(timestamp)})
        remove("{0}{1}_classic.gexf".format(graphs_folder, timestamp))


def load_gephx_sequence(graphs_folder):
    graph_files = [graphs_folder + f for f in listdir(graphs_folder)]
    graph_sequence = []
    for f in graph_files:
        graph_sequence.append(nx.read_gexf(f))
    return graph_sequence

def get_taxonomy_type(image_name):
    taxonomy = {
        'google/cadvisor': 'MONITOR',
        'prom/node-exporter': 'MONITOR',
        'alvarobrandon/fmone-agent': 'CLIENT',
        'ches/kafka': 'BACK_END',
        'zookeeper': 'BACK_END',
        'mesosphere/marathon:v1.5.1.1': 'FRONT_END',
        'host' : 'HOST',
        'sysdig/sysdig' : 'MONITOR'
    }
    return taxonomy.get(image_name,'UNKNOWN')

def get_node_type(metric):
    # return the taxonomy type for this metric. For the moment the taxonomy is based on the image name
    # we assume that if it doesn't have an image then it's a host
    return get_taxonomy_type(metric.get('image','host'))

def categorize_nodes(graph):
    for node, attributes in graph.nodes(data=True):
        graph.add_node(node, attr_dict={'type' : get_node_type(attributes)})


def create_prometheus_node(container_id,element_info,graph):
    """
    Create a node that is inserted into the graph with the information contained in a DF that has two indexes:
    metric_type with the type of metric and host with the host where this metric was scraped from
    :param element_info: we get data through inplugin.collect() every coll_period secs
    :type element_info: DataFrame
    """
    for idx, row in element_info.iterrows(): # I have a node to create
        # metric is a tuple that has as the first element the remaining index elements. Note how we eliminated the time
        # dimension since this is a snapshot, and we also eliminated the id dimension since we are slicing for each.
        # the two dimensions left are the type of metric and the host.
        # The second element of the tuple is a pd.series. It has the value for the type of metric and a dict with information
        # inserted by the scrapper for that metric
        graph.add_node(container_id,attr_dict={idx[0] : optional_float(row.value)}) # the metric cloud be a string or float
        # some metrics have specific information about that container or element id in a dict. Things like the name or the
        # mesos task ID. This is contained in metric[1][1] and we are going to store that in the node as well. Note how usually these are repeated for the
        # same scrapper. For example cadvisor is always going to include mesos task ID if the container was launched
        # in Mesos. One thing we could do could be grouping by metric column but it's not hashable and so, we just call
        # the add_attribute function for each dict
        graph.add_node(container_id,attr_dict=row.metric)
        # there is also the need to add the type of node it is ('backend','frontend'...)
        graph.add_node(container_id,attr_dict={'type' : get_node_type(row.metric)})
        # Finally we add an edge from the host to the container that runs on it.
        # NOTE: in the case of node_exporter this creates a self loop since the container_id is the same as the scraped host
        graph.add_edge(idx[1],container_id)
        graph.add_edge(container_id, idx[1])

def add_prometheus_information(graph,prom_snapshot):
    # use this query to get all the distinct values of images for this experiment set([d.get('image') for d in prom_snapshot['metric'].values])
    for container_id in prom_snapshot.index.get_level_values("id").values: # for each id in the index
        create_prometheus_node(container_id,element_info=prom_snapshot.xs(container_id,level="id"),graph=graph) # I take its information and I create a node in the graph

def add_sysdig_information(graph,sysdig_snapshot):
    for idx, df_comm in sysdig_snapshot.reset_index(level=range(4, sysdig_snapshot.index.names.__len__())).groupby(level=[0, 1, 2, 3]):
        if df_comm.shape[0] == 2:  # if there is only one node in our monitred data... (this will possibly be avoided when we include the native host processes)
            bytes_read, bytes_write = tuple(df_comm['sum'].values)
            req_read, req_write = tuple(df_comm['count'].values)
            # the destination of the edge is just the machine it communicates with, that is idx[1]
            if '9.0.' in idx[1]:
                print('There is an unkwnon container')
            graph.add_edge(df_comm['container.id'][0], idx[1], bytes_read=int(bytes_read), bytes_write=int(bytes_write),
                        req_read=int(req_read), req_write=int(req_write))
        if df_comm.shape[0]==4: # if the two nodes involved in the tcp pipe are present on the data
            for source_name in df_comm['container.id'].unique(): # for each container id in the pipe communication df
                # we build a tuple with the list of values that represent the sum column with the read and written bytes
                bytes_read, bytes_write =  tuple(df_comm[df_comm['container.id'] == source_name]['sum'].values)
                # same with the number of requests
                req_read, req_write = tuple(df_comm[df_comm['container.id'] == source_name]['count'].values)
                for dest_name in df_comm[df_comm['container.id']!=source_name]['container.id']: # for the destination (the container that is not the source)
                    graph.add_edge(source_name,dest_name,bytes_read=int(bytes_read),bytes_write=int(bytes_write),req_read=int(req_read),req_write=int(req_write))


def build_graph_for_snapshot(prom_snapshot,sysdig_snapshot):
    DG = nx.DiGraph()
    if prom_snapshot is not None:
        add_prometheus_information(DG,prom_snapshot)
    if sysdig_snapshot is not None:
        add_sysdig_information(DG,sysdig_snapshot)
    nx.set_node_attributes(DG,'anomalies',[])
    nx.set_node_attributes(DG,'anomaly_level',1)
    nx.set_edge_attributes(DG,'anomalies',[])
    nx.set_edge_attributes(DG,'anomaly_level',1)
    DG.remove_edges_from(DG.selfloop_edges())  # we remove the self loops that are formed with node_exporter
    categorize_nodes(DG)
    return DG

def tag_anomalous_nodes(graph_sequence,experiment_log):
    # flatten = lambda l: [item for sublist in l for item in sublist]
    for idx, row in experiment_log[experiment_log['type']=='anomaly'].iterrows():
        anomalous_graphs = {k: v for k, v in graph_sequence.iteritems() if k in xrange(row.date_start, row.date_end)}
        for t, AG in anomalous_graphs.iteritems():
            # if list(row.nodes)[0] ==  'marathon-lb.marathon.mesos':
            #     print "Search for the LB node and mark it as anomalous plis"
            if type(row.nodes) is str: # fix for when the user in execo pass a string as a parameter to nodes
                row.nodes = {row.nodes}
            anomalous_edges = AG.edges(row.nodes)
            nx.set_node_attributes(AG,'anomalies',dict(zip(row.nodes,[row.event + ':' + row.aditional_info] * len(row.nodes))))
            nx.set_node_attributes(AG,'anomaly_level', dict(zip(row.nodes, [3] * len(row.nodes))))
            nx.set_edge_attributes(AG,'anomalies',dict(zip(anomalous_edges,[row.event + ':' + row.aditional_info] * len(anomalous_edges))))
            nx.set_edge_attributes(AG,'anomaly_level', dict(zip(anomalous_edges, [3] * len(anomalous_edges))))


def build_graph_sequence(start,end,step,prometheus_path,sysdig_path,anomalies_file):
    """
    Build a dictionary where keys are timestamps and values are networkX graphs. The graphs are built
    from monitored prometheus and sysdig data ranging from start to end timestamps.
    In addition anomalous nodes are tagged with the data contained in anomalies_file
    :param start: timestamp
    :param end: timestamp
    :param step: seconds. Used by prometheus to do perform queries between ranges e.g. 1s
    :param prometheus_path: the url of the prometheus server
    :param sysdig_path: a path in the local filesystem with the sysdig.scrap files
    :param anomalies_file: A file that contains the anomalies for a given experiment
    :return:
    """
    graph_sequence = {}
    prom_df = create_prometheus_df(start,end,step,prometheus_path)
    sysdig_df = create_sysdig_df(start,end,sysdig_path)
    for timestamp in xrange(int(start),int(end)):
        try:
            sysdig_snapshot = sysdig_df.xs(timestamp,level='evt.rawtime.s')
        except KeyError:
            sysdig_snapshot = None
        try:
            prom_snapshot = prom_df.xs(timestamp,level='time')
        except KeyError:
            prom_snapshot = None
        G = build_graph_for_snapshot(prom_snapshot=prom_snapshot,
                                     sysdig_snapshot=sysdig_snapshot)
        graph_sequence[timestamp] = G
    experiment_log = pd.read_pickle(anomalies_file)
    tag_anomalous_nodes(graph_sequence, experiment_log)
    return graph_sequence


if __name__ == '__main__':
    """
    Several things are done within the main program thread
    1. Read the information to build the graph from : 
        - experiment_res_path: sysdig files, experiment_log.pickle
        - prometheus_path: container metrics
    2. Export the graph sequence built to gephx_output_path as a sequence of Gephi .gephx files
    3. Export the graph sequence dictionary as a pickle into the experiment_res_path
    So at the end we will have outputs in
        - experiment_res_path (execo_results): the graph sequence in a pickle format
        - gephx_output_path (RCAGephi): the graph sequence as a pickle
    Note how in the RCAGephi path we will also have the matchings and the patterns that will be created 
    by the rca_engine contained in a different python module
    """
    name = "second_fuzzy_cpu"
    mongodb = 'localhost'
    start = 1512053561 - 7
    end = 1512053591 + 7
    step = '1s'
    # the folder where the results of the experiment are
    experiments_res_path = '/Users/alvarobrandon/execo_experiments/first_evaluation_rca/'
    # the prometheus server from where we are going to get the metrics
    prometheus_path = 'http://fnancy.nancy.grid5000.fr:9090/api/v1/query_range'
    # the sysdig metrics can be found on the experiment folder
    sysdig_path = experiments_res_path
    # the anomalies file is also on the experiment folder
    anomalies_file = experiments_res_path + 'experiment_log.pickle'
    # here we are going to dump the array of networX graphs that will be inserted into graph.timestamp
    gephx_output_path = '/Users/alvarobrandon/RCAGephi/' + name + "/graph_sequence"
    if not exists(gephx_output_path):
        makedirs(gephx_output_path)
    # timestamp = 1507561419  # What happened this second?. What containers where active and what were their metrics?
    # graph_sequence = load_gephx_sequence(output_path)
    # graph_sequence = pickle.load(open(sysdig_path + 'graph_sequence.pickle', 'rb'))
    graph_sequence = build_graph_sequence(start,end,step,prometheus_path,sysdig_path,anomalies_file)
    create_gephx_sequence(graph_sequence,gephx_output_path)
    mongodb_insert_graph_seq(mongodb,graph_sequence,name,name)
    pickle.dump(graph_sequence,open(experiments_res_path + '{0}.pickle'.format(name), 'wb'))
