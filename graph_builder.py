from prometheus_df_builder import create_prometheus_df
from sysdig_df_builder import create_sysdig_df
import networkx as nx
from os import makedirs, listdir, remove
from os.path import exists, expanduser
from mongo_exporter import mongodb_insert_graph_seq
import pandas as pd
import pickle
from numpy import unique
import re
from collections import Counter
import time
current_milli_time = lambda: int(round(time.time() * 1000))

old_names_start_end = [
    ('first_cpu_kafka', 1516874441 - 5, 1516874461 + 5),
    ('second_cpu_kafka', 1516874493 - 5, 1516874513 + 5),
    ('third_cpu_kafka', 1516874607 - 5, 1516874627 + 5),
    ('first_bandwidth75_kafka', 1516874660 - 5, 1516874680 + 5),
    ('second_bandwidth75_kafka', 1516874748 - 5, 1516874768 + 5),
    ('third_bandwidth75_kafka', 1516874826 - 5, 1516874846 + 5),
    ('first_disk_kafka', 1516874914 - 5, 1516874934 + 5),
    ('second_disk_kafka', 1516874986 - 5, 1516875006 + 5),
    ('third_disk_kafka', 1516875029 - 5, 1516875049 + 5),
    ('first_network_kafka', 1516875077 - 5, 1516875097 + 5),
    ('second_network_kafka', 1516875133 - 5, 1516875153 + 5),
    ('third_network_kafka', 1516875190 - 5, 1516875210 + 5),
    ('first_bigheap_kafka', 1516875260 - 5, 1516875275 + 5),
    ('second_bigheap_kafka', 1516875312 - 5, 1516875327 + 5),
    ('third_bigheap_kafka', 1516875367 - 5, 1516875382 + 5),
    ('first_zlib_kafka', 1516875421 - 5, 1516875441 + 5),
    ('second_zlib_kafka', 1516875471 - 5, 1516875491 + 5),
    ('third_zlib_kafka', 1516875505 - 5, 1516875525 + 5),
    ('first_cpu_lb', 1516876149 - 5, 1516876169 + 5),
    ('second_cpu_lb', 1516876197 - 5, 1516876217 + 5),
    ('third_cpu_lb', 1516876253 - 5, 1516876273 + 5),
    ('first_bandwidth75_lb', 1516876296 - 5, 1516876316 + 5),
    ('second_bandwidth75_lb', 1516876404 - 5, 1516876424 + 5),
    ('third_bandwidth75_lb', 1516876523 - 5, 1516876543 + 5),
    ('first_disk_lb', 1516876617 - 5, 1516876637 + 5),
    ('second_disk_lb', 1516876663 - 5, 1516876683 + 5),
    ('third_disk_lb', 1516876718 - 5, 1516876738 + 5),
    ('first_network_lb', 1516876780 - 5, 1516876800 + 5),
    ('second_network_lb', 1516876862 - 5, 1516876882 + 5),
    ('third_network_lb', 1516876953 - 5, 1516876973 + 5),
    ('first_bigheap_lb', 1516877023 - 5, 1516877038 + 5),
    ('second_bigheap_lb', 1516877088 - 5, 1516877103 + 5),
    ('third_bigheap_lb', 1516877168 - 5, 1516877183 + 5),
    ('first_zlib_lb', 1516877574 - 5, 1516877594 + 5),
    ('second_zlib_lb', 1516877609 - 5, 1516877629 + 5),
    ('third_zlib_lb', 1516877658 - 5, 1516877678 + 5),
    ('first_wrong_lb_conf', 1516878833 - 5, 1516878843),
    ('second_wrong_lb_conf', 1516878843, 1516878853),
    ('third_wrong_lb_conf', 1516878853, 1516878863),
    ('first_stress_endpoint_lb', 1516879061 - 5, 1516879081 + 5),
    ('second_stress_endpoint_lb', 1516879224 - 5, 1516879244 + 5),
    ('third_stress_endpoint_lb', 1516879383 - 5, 1516879403 + 5)
]

random_start_end = [
    ('first_cpu_lb_random', 1519120696 - 5, 1519120716 + 5),
    ('second_cpu_lb_random', 1519120727 - 5, 1519120747 + 5),
    ('third_cpu_lb_random', 1519120759 - 5, 1519120779 + 5),
    ('first_bandwidth75_lb_random', 1519120826 - 5, 1519120846 + 5),
    ('second_bandwidth75_lb_random', 1519120899 - 5, 1519120919 + 5),
    ('third_bandwidth75_lb_random', 1519120981 - 5, 1519121001 + 5),
    ('first_disk_lb_random', 1519121164 - 5, 1519121184 + 5),
    ('second_disk_lb_random', 1519121209 - 5, 1519121229 + 5),
    ('third_disk_lb_random', 1519121253 - 5, 1519121273 + 5),
    ('first_network_lb_random', 1519121296 - 5, 1519121316 + 5),
    ('second_network_lb_random', 1519121338 - 5, 1519121358 + 5),
    ('third_network_lb_random', 1519121371 - 5, 1519121391 + 5),
    ('first_bigheap_lb_random', 1519121443 - 5, 1519121458 + 5),
    ('second_bigheap_lb_random', 1519121478 - 5, 1519121493 + 5),
    ('third_bigheap_lb_random', 1519121506 - 5, 1519121521 + 5),
    ('first_zlib_lb_random', 1519121578 - 5, 1519121598 + 5),
    ('second_zlib_lb_random', 1519121625 - 5, 1519121645 + 5),
    ('third_zlib_lb_random', 1519121689 - 5, 1519121709 + 5),
    ('first_wrong_lb_conf_random', 1519122188 - 5, 1519122208),
    ('second_wrong_lb_conf_random', 1519122208, 1519122228),
    ('third_wrong_lb_conf_random', 1519122228, 1519122248),
    ('first_stress_endpoint_lb_random', 1519122653 - 5, 1519122673 + 5),
    ('second_stress_endpoint_lb_random', 1519122792 - 5, 1519122812 + 5),
    ('third_stress_endpoint_lb_random', 1519122901 - 5, 1519122921 + 5)
]

def optional_float(f):
    try:
        return float(f)
    except ValueError:
        return f

def print_different_images(prom_df):
    """
    An auxiliary function that prints all the different Docker images used in an scenario. This is useful if you want
    to build a taxonomy for the containers used each of the scenarios (kafka, loadbalancer ...)
    :param prom_df:
    """
    images = []
    for idx, row in prom_df.iterrows():
        images.append(row.metric.get('image'))
    print(set(images))

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
                       {classic_header: new_header.format(timestamp)})
        remove("{0}{1}_classic.gexf".format(graphs_folder, timestamp))


def load_gephx_sequence(graphs_folder):
    graph_files = [graphs_folder + f for f in listdir(graphs_folder)]
    graph_sequence = []
    for f in graph_files:
        graph_sequence.append(nx.read_gexf(f))
    return graph_sequence


def get_taxonomy_type(image_name):
    taxonomy = {
        'google/cadvisor:latest': 'MONITOR',
        'prom/node-exporter': 'MONITOR',
        'alvarobrandon/fmone-agent': 'CLIENT',
        'ches/kafka': 'BACK_END',
        'zookeeper': 'BACK_END',
        'mesosphere/marathon:v1.5.6': 'FRONT_END',
        'host': 'HOST',
        'sysdig/sysdig': 'MONITOR',
        'wordpress': 'BACK_END',
        'yokogawa/siege': 'CLIENT',
        'jordi/ab' : 'CLIENT',
        'mesosphere/marathon-lb:v1.11.1': 'FRONT_END',
        'mysql': 'BACK_END',
        'alvarobrandon/spark-worker' : 'BACK_END',
        'uhopper/hadoop-datanode:2.8.1' : 'BACK_END',
        'alvarobrandon/spark-master' : 'FRONT_END',
        'uhopper/hadoop-namenode:2.8.1' : 'FRONT_END',
        'alvarobrandon/spark-bench' : 'CLIENT',
        'unknown' : 'UNKNOWN'
    }
    # if the label is not here then we don't know what it is (N/A)
    taxonomy_type = taxonomy.get(image_name,'N/A')
    return taxonomy_type


def get_node_type(metric):
    # return the taxonomy type for this metric. For the moment the taxonomy is based on the image name
    return get_taxonomy_type(metric.get('image'))


def categorize_nodes(graph):
    for node, attributes in graph.nodes(data=True):
        graph.add_node(node, attr_dict={'type': get_node_type(attributes)})
        # Apart from that if the type of the node is unknown then give a name to the image. We do so for Gephi visual
        if get_node_type(attributes)=="N/A":
            graph.add_node(node, attr_dict={'image': 'N/A'})



def create_prometheus_node(container_id, element_info, graph):
    """
    Create a node that is inserted into the graph with the information contained in a DF that has two indexes:
    metric_type with the type of metric and host with the host where this metric was scraped from
    :param element_info: we get data through inplugin.collect() every coll_period secs
    :type element_info: DataFrame
    """
    for idx, row in element_info.iterrows():  # I have a node to create
        # metric is a tuple that has as the first element the remaining index elements. Note how we eliminated the time
        # dimension since this is a snapshot, and we also eliminated the id dimension since we are slicing for each.
        # the two dimensions left are the type of metric and the host.
        # The second element of the tuple is a pd.series. It has the value for the type of metric and a dict with information
        # inserted by the scrapper for that metric
        graph.add_node(container_id,
                       attr_dict={idx[0]: optional_float(row.value)})  # the metric cloud be a string or float
        # some metrics have specific information about that container or element id in a dict. Things like the name or the
        # mesos task ID. This is contained in metric[1][1] and we are going to store that in the node as well. Note how usually these are repeated for the
        # same scrapper. For example cadvisor is always going to include mesos task ID if the container was launched
        # in Mesos. One thing we could do could be grouping by metric column but it's not hashable and so, we just call
        # the add_attribute function for each dict
        graph.add_node(container_id, attr_dict=row.metric)
        # if there is not an image entry for the metric then it should be a host. This is a fix to be able to
        # visualise hosts in Gephi
        if not row.metric.get('image'):
            graph.add_node(container_id,attr_dict={'image' : 'host'})
        # Finally we add an edge from the host to the container that runs on it.
        # NOTE: in the case of node_exporter this creates a self loop since the container_id is the same as the scraped host
        graph.add_edge(idx[1], container_id)
        graph.add_edge(container_id, idx[1])
        # Sometimes the host metrics are not available. Since we add an edge from a container to the host (idx[1])
        # it resides, we also give an image entry to this node to be used by the categorise_nodes process
        graph.add_node(idx[1], attr_dict={'image': 'host'})


def add_prometheus_information(graph, prom_snapshot):
    # use this query to get all the distinct values of images for this experiment set([d.get('image') for d in prom_snapshot['metric'].values])
    for container_id in prom_snapshot.index.get_level_values("id").values:  # for each id in the index
        create_prometheus_node(container_id, element_info=prom_snapshot.xs(container_id, level="id"),
                               graph=graph)  # I take its information and I create a node in the graph


def add_sysdig_information(graph, sysdig_snapshot):
    def add_edge_values(row, idx, df_comm, graph):
        external_agents = ['172.16.45.2']
        # the source of the edge is always going to be the containerid for this entry
        source_name = row['container.id']
        # if for this df_comm we have only one container involved (the difference with the source is empty)
        if df_comm[df_comm['container.id'] != source_name]['container.id'].empty:
            # if the information is coming from an external agent then we have only data for the information
            # that flows towards that external agent and so the destination will be this external IP
            if idx[0] in external_agents:
                dest_name = idx[0]  # the destination is the external node
            else:
                # if it's an internal IP and it's a one way communication
                # the destination of the edge is just the machine it communicates with, that is idx[1]
                dest_name = idx[1]
                is_unknown_ip = re.match(r'9\.0\.(?:[0-9]+\.*){2}',dest_name)
                if is_unknown_ip:
                    print('There is an unkwnon container: communication from {0} -> {1} inside df_comm {2}'.format(
                        source_name, dest_name, df_comm))
                    # we add the node as an unknwon type. We will add the edge further down in the common logic.
                    # graph.add_node(dest_name, attr_dict={'image': 'unknown'})
                    # we change to a version where an unknown communication attribute is added
                    dest_name = None
                    graph.add_node(source_name,attr_dict={'delay': 'true'})
        else:
            dest_name = df_comm[df_comm['container.id'] != source_name]['container.id'].unique()[0]
        # logic to determine if this entry is a read or write tcp request
        if dest_name is not None:
            if row['evt.io_dir'] == 'write':
                graph.add_edge(source_name, dest_name,
                               bytes_write=int(row['sum']),
                               req_write=int(row['count']))
            if row['evt.io_dir'] == 'read':
                graph.add_edge(source_name, dest_name,
                               bytes_read=int(row['sum']),
                               req_read=int(row['count']))
        # we will also add all the information we have from the sysdig entry to the nodes of the graphs
        # we do so, because we notice that at times we have entries for sysdig but not for prometheus
        # The information we can have in sysdig and not in prometheus is 1. The container.image 2. The relation between
        # container and host
        graph.add_node(source_name, attr_dict={'image' : row['container.image']}) # the docker image
        graph.add_edge(source_name, row['evt.host']) # the relation between container and host
        graph.add_edge(row['evt.host'], source_name)
        graph.add_node(row['evt.host'], attr_dict={'image': 'host'})


    def process_df_comm(idx, df_comm, graph):
        # There can be four different types of dimensions for the DF_COMM depending on the sysdig data captured:
        # 1. two rows. This means that sysdig could only capture the communication through the pipe in one of the sides
        # e.g. marathon-user container which communicates with the mesos-agent that is not currently captured by sysdig
        # 2. four rows. This means that we have the data for the two ends of the communication.
        # e.g. Two containers communicating with each other
        # 3. one row. This means that we only have the data for the read or write part of the communication in ONLY one
        # of the ends of the line. e.g. A HTTP requests that was sent but the confirmation takes too long to travel back
        # this happens also when the confirmation of a TCP write comes back (is captured) in the following snapshot
        # 4. Any other case. This TCP pipe is repeated across many nodes. e.g. Requests from localhost to the
        # embedded DNS docker server
        dns_lookups = ['127.0.0.11', '127.0.0.1']
        # This variable is going to represent the number of containers involved in this TCP pipe
        tcp_pipe_conts = len(unique(df_comm['container.id'].values))
        if idx[1] not in dns_lookups:  # if the call doesn't involve DNS lookups #TODO Maybe consider these DNS too
            if tcp_pipe_conts > 2:  # Something's wrong. In a TCP pipe there can only be two containers involved
                print(
                    "Special entry with {0} rows in df_comm between {1} -> {2}".format(df_comm.shape[0], idx[0],
                                                                                       idx[1]))
                # There can be several reasons for this. The ones we have seen are calls to localhost for DNS resolving
                # or containers that have the same IP through different hosts.
                # The solution is to break it into smaller df_comms by container and host and process it
                for idx2, df_comm2 in df_comm.groupby(['container.id', 'evt.host']):
                    # idx2 will be a tuple of container.id and host.
                    # we need a new_idx that is the already present index of the original df_comm
                    new_idx = df_comm2.index.values[0]
                    process_df_comm(new_idx, df_comm2, graph)
            else:  # if there is one or two containers involved in the tcp pipe
                # for each of the container entries
                for row in df_comm.iterrows():
                    # add the df_comm information to the graph for this particular row
                    add_edge_values(row[1], idx, df_comm, graph)

    # each df_comm represents a communication between two IP's e.g. container1IP -> container2IP.
    # idx represents the ip of the client in idx[0] and the server in idx[1]
    for idx, df_comm in sysdig_snapshot.reset_index(level=range(2, sysdig_snapshot.index.names.__len__())).groupby(
            level=[0, 1]):
        process_df_comm(idx, df_comm, graph)


def build_graph_for_snapshot(prom_snapshot, sysdig_snapshot):
    DG = nx.DiGraph()
    if prom_snapshot is not None:
        add_prometheus_information(DG, prom_snapshot)
    if sysdig_snapshot is not None:
        add_sysdig_information(DG, sysdig_snapshot)
    nx.set_node_attributes(DG, 'anomalies', [])
    nx.set_node_attributes(DG, 'anomaly_level', 1)
    nx.set_edge_attributes(DG, 'anomalies', [])
    nx.set_edge_attributes(DG, 'anomaly_level', 1)
    DG.remove_edges_from(DG.selfloop_edges())  # we remove the self loops that are formed with node_exporter
    categorize_nodes(DG)
    return DG

def nodes_with_conn_problems(AG,threshold):
    unknwown_ids = [nid for nid, attrdict in AG.nodes(data=True) if attrdict.get('image')=='unknown']
    incoming_nodes = [nodeid for nodeid, dest in AG.in_edges(unknwown_ids)]
    count = Counter(incoming_nodes)
    anomalous_nodes = [k for k, v in count.iteritems() if v >= threshold]
    if anomalous_nodes:
        nx.set_node_attributes(AG, 'anomalies',
                               dict(zip(anomalous_nodes,
                                        ["Number of unknown connections > {0}".format(threshold)] * len(anomalous_nodes))
                                    )
                               )
        nx.set_node_attributes(AG, 'anomaly_level', dict(zip(anomalous_nodes, [3] * len(anomalous_nodes))))
        anomalous_edges = AG.edges(anomalous_nodes)
        nx.set_edge_attributes(AG, 'anomalies',
                               dict(zip(anomalous_edges,
                                        ["Number of unknown connections > {0}".format(threshold)] * len(anomalous_edges))
                                    )
                               )
        nx.set_edge_attributes(AG, 'anomaly_level', dict(zip(anomalous_edges, [3] * len(anomalous_edges))))



def tag_anomalous_nodes(graph_sequence, experiment_log):
    # flatten = lambda l: [item for sublist in l for item in sublist]
    for idx, row in experiment_log[experiment_log['type'] == 'anomaly'].iterrows():
        anomalous_graphs = {k: v for k, v in graph_sequence.iteritems() if
                            k in xrange(row.date_start + 7, row.date_end)}
        for t, AG in anomalous_graphs.iteritems():
            # if list(row.nodes)[0] ==  'marathon-lb.marathon.mesos':
            #     print "Search for the LB node and mark it as anomalous plis"
            if type(row.nodes) is str:  # fix for when the user in execo pass a string as a parameter to nodes
                if row.nodes == 'marathon-lb.marathon.mesos':
                    row.nodes = [contid for contid, attributes in AG.nodes(data=True) if
                                 attributes.get('image') == 'mesosphere/marathon-lb:v1.11.1']
                else:
                    row.nodes = {row.nodes}
            anomalous_edges = AG.edges(row.nodes)
            # Additionally, we are going to search for nodes that have number of unknown connections above one threshold
            # nodes_with_conn_problems(AG,threshold=3)
            try:
                nx.set_node_attributes(AG, 'anomalies',
                                       dict(zip(row.nodes, [row.event + ':' + row.aditional_info] * len(row.nodes))))
                nx.set_node_attributes(AG, 'anomaly_level', dict(zip(row.nodes, [3] * len(row.nodes))))
                nx.set_edge_attributes(AG, 'anomalies', dict(
                    zip(anomalous_edges, [row.event + ':' + row.aditional_info] * len(anomalous_edges))))
                nx.set_edge_attributes(AG, 'anomaly_level', dict(zip(anomalous_edges, [3] * len(anomalous_edges))))
            except KeyError:
                print("Impossible to set the anomaly {0} in the node {1}: The node does not exist"
                      .format(row.event,row.nodes))

def create_prometheus_df_backup(start, end, step, prometheus_path, file_out):
    prom_df = create_prometheus_df(start, end, step, prometheus_path)
    prom_df.to_pickle(file_out)


def build_graph_sequence(start, end, prometheus_df, huge_sysdig_df, anomalies_file,exec_profile):
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
    # optional: check the different docker images captured by Prometheus in this scenario
    # print_different_images(prom_df)
    for timestamp in xrange(int(start), int(end)):
        try:
            sysdig_snapshot = huge_sysdig_df.xs(timestamp, level='evt.rawtime.s')
        except KeyError:
            sysdig_snapshot = None
        try:
            prom_snapshot = prometheus_df.xs(timestamp, level='time')
        except KeyError:
            prom_snapshot = None
        t1 = current_milli_time()
        G = build_graph_for_snapshot(prom_snapshot=prom_snapshot,
                                     sysdig_snapshot=sysdig_snapshot)
        t2 = current_milli_time()
        graph_sequence[timestamp] = G
        nnodes = G.number_of_nodes()
        if nnodes not in exec_profile:
            exec_profile[nnodes] = []
        exec_profile[nnodes].append(t2-t1)
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
    exec_profile = {}
    names_start_end = [
        ('one_cpu_lb_random', 1521806868 - 5, 1521806883 + 5),
        ('two_cpu_lb_random', 1521806895 - 5, 1521806910 + 5),
        ('three_cpu_lb_random', 1521806932 - 5, 1521806947 + 5),
        ('four_cpu_lb_random', 1521806973 - 5, 1521806988 + 5),
        ('five_cpu_lb_random', 1521807006 - 5, 1521807021 + 5),
        ('six_cpu_lb_random', 1521807053 - 5, 1521807068 + 5),
        ('one_band_lb_random', 1521807081 - 5, 1521807096 + 5),
        ('two_band_lb_random', 1521807156 - 5, 1521807171 + 5),
        ('three_band_lb_random', 1521807242 - 5, 1521807257 + 5),
        ('four_band_lb_random', 1521807315 - 5, 1521807330 + 5),
        ('five_band_lb_random', 1521807389 - 5, 1521807404 + 5),
        ('six_band_lb_random', 1521807458 - 5, 1521807473 + 5),
        ('one_disk_lb_random', 1521807533 - 5, 1521807548 + 5),
        ('two_disk_lb_random', 1521807557 - 5, 1521807572 + 5),
        ('three_disk_lb_random', 1521807597 - 5, 1521807612 + 5),
        ('four_disk_lb_random', 1521807622 - 5, 1521807637 + 5),
        ('five_disk_lb_random', 1521807645 - 5, 1521807660 + 5),
        ('six_disk_lb_random', 1521807673 - 5, 1521807688 + 5),
        ('one_bigheap_lb_random', 1521807697 - 5, 1521807712 + 5),
        ('two_bigheap_lb_random', 1521807728 - 5, 1521807743 + 5),
        ('three_bigheap_lb_random', 1521807764 - 5, 1521807779 + 5),
        ('four_bigheap_lb_random', 1521807794 - 5, 1521807809 + 5),
        ('five_bigheap_lb_random', 1521807827 - 5, 1521807842 + 5),
        ('six_bigheap_lb_random', 1521807858 - 5, 1521807873 + 5)
]
    mongodb = 'localhost'
    step = '1s'
    # the folder where the results of the experiment are
    experiments_res_path = '/Users/alvarobrandon/execo_experiments/gold_lb_random_host_anomalies/'
    # the prometheus server from where we are going to get the metrics
    prometheus_path = 'http://abrandon-vm.lille.grid5000.fr:9090/api/v1/query_range'
    # the sysdig metrics can be found on the experiment folder
    sysdig_path = experiments_res_path
    # building the sysdig dataframe is a very expensive process. We build it once and then we use the index to select
    # the data. Note how before we have a create_sysdig_df method that took start and end parameters to narrow the data
    # size
    #huge_sysdig_df = create_sysdig_df(sysdig_path)
    #huge_sysdig_df.to_pickle(experiments_res_path + 'huge_sysdig_df.pickle')
    huge_sysdig_df = pd.read_pickle(experiments_res_path + 'huge_sysdig_df.pickle')
    prom_df = pd.read_pickle(experiments_res_path + 'huge_prom_df.pickle')
    # the anomalies file is also on the experiment folder
    anomalies_file = experiments_res_path + 'experiment_log.pickle'
    for name, start, end in names_start_end:
        # here we are going to dump the array of networX graphs that will be inserted into graph.timestamp
        gephx_output_path = '/Users/alvarobrandon/RCAGephi/' + name + "/graph_sequence/"
        if not exists(gephx_output_path):
            makedirs(gephx_output_path)
        # timestamp = 1507561419  # What happened this second?. What containers where active and what were their metrics?
        # graph_sequence = load_gephx_sequence(output_path)
        # graph_sequence = pickle.load(open(sysdig_path + 'graph_sequence.pickle', 'rb'))
        # prom_df = create_prometheus_df(start, end, step, prometheus_path)
        graph_sequence = build_graph_sequence(start, end, prom_df, huge_sysdig_df, anomalies_file,exec_profile)
        create_gephx_sequence(graph_sequence, gephx_output_path)
        mongodb_insert_graph_seq(mongodb, graph_sequence, name, name)
        pickle.dump(graph_sequence, open(experiments_res_path + '{0}.pickle'.format(name), 'wb'))
    # create_prometheus_df_backup(1521796996 - 10, 1521798384 + 10, step, prometheus_path,
    #                             experiments_res_path + 'huge_prom_df.pickle')
#     names_start_end = [
#         ('one_cpu_spark_random', 1521796996 - 5, 1521797011 + 5),
#         ('two_cpu_spark_random', 1521797028 - 5, 1521797043 + 5),
#         ('three_cpu_spark_random', 1521797057 - 5, 1521797072 + 5),
#         ('four_cpu_spark_random', 1521797115 - 5, 1521797130 + 5),
#         ('five_cpu_spark_random', 1521797164 - 5, 1521797179 + 5),
#         ('six_cpu_spark_random', 1521797201 - 5, 1521797216 + 5),
#         ('one_band_spark_random', 1521797268 - 5, 1521797283 + 5),
#         ('two_band_spark_random', 1521797357 - 5, 1521797372 + 5),
#         ('three_band_spark_random', 1521797440 - 5, 1521797455 + 5),
#         ('four_band_spark_random', 1521797553 - 5, 1521797568 + 5),
#         ('five_band_spark_random', 1521797741 - 5, 1521797756 + 5),
#         ('six_band_spark_random', 1521797776 - 5, 1521797791 + 5),
#         ('one_disk_spark_random', 1521797913 - 5, 1521797928 + 5),
#         ('two_disk_spark_random', 1521797949 - 5, 1521797964 + 5),
#         ('three_disk_spark_random', 1521797978 - 5, 1521797993 + 5),
#         ('four_disk_spark_random', 1521798042 - 5, 1521798057 + 5),
#         ('five_disk_spark_random', 1521798096 - 5, 1521798111 + 5),
#         ('six_disk_spark_random', 1521798128 - 5, 1521798143 + 5),
#         ('one_bigheap_spark_random', 1521798171 - 5, 1521798186 + 5),
#         ('two_bigheap_spark_random', 1521798200 - 5, 1521798215 + 5),
#         ('three_bigheap_spark_random', 1521798242 - 5, 1521798257 + 5),
#         ('four_bigheap_spark_random', 1521798276 - 5, 1521798291 + 5),
#         ('five_bigheap_spark_random', 1521798332 - 5, 1521798347 + 5),
#         ('six_bigheap_spark_random', 1521798369 - 5, 1521798384 + 5)
# ]
#     mongodb = 'localhost'
#     step = '1s'
#     # the folder where the results of the experiment are
#     experiments_res_path = '/Users/alvarobrandon/execo_experiments/gold_spark_random/'
#     # the prometheus server from where we are going to get the metrics
#     prometheus_path = 'http://abrandon-vm.lille.grid5000.fr:9090/api/v1/query_range'
#     # the sysdig metrics can be found on the experiment folder
#     sysdig_path = experiments_res_path
#     # building the sysdig dataframe is a very expensive process. We build it once and then we use the index to select
#     # the data. Note how before we have a create_sysdig_df method that took start and end parameters to narrow the data
#     # size
#     # huge_sysdig_df = create_sysdig_df(sysdig_path)
#     # huge_sysdig_df.to_pickle(experiments_res_path + 'huge_sysdig_df.pickle')
#     huge_sysdig_df = pd.read_pickle(experiments_res_path + 'huge_sysdig_df.pickle')
#     prom_df = pd.read_pickle(experiments_res_path + 'huge_prom_df.pickle')
#     # the anomalies file is also on the experiment folder
#     anomalies_file = experiments_res_path + 'experiment_log.pickle'
#     for name, start, end in names_start_end:
#         # here we are going to dump the array of networX graphs that will be inserted into graph.timestamp
#         gephx_output_path = '/Users/alvarobrandon/RCAGephi/' + name + "/graph_sequence/"
#         if not exists(gephx_output_path):
#             makedirs(gephx_output_path)
#         # timestamp = 1507561419  # What happened this second?. What containers where active and what were their metrics?
#         # graph_sequence = load_gephx_sequence(output_path)
#         # graph_sequence = pickle.load(open(sysdig_path + 'graph_sequence.pickle', 'rb'))
#         # prom_df = create_prometheus_df(start, end, step, prometheus_path)
#         graph_sequence = build_graph_sequence(start, end, prom_df, huge_sysdig_df, anomalies_file,exec_profile)
#         create_gephx_sequence(graph_sequence, gephx_output_path)
#         mongodb_insert_graph_seq(mongodb, graph_sequence, name, name)
#         pickle.dump(graph_sequence, open(experiments_res_path + '{0}.pickle'.format(name), 'wb'))
#     # create_prometheus_df_backup(1521796996 - 10, 1521798384 + 10, step, prometheus_path,
#     #                             experiments_res_path + 'huge_prom_df.pickle')
#     names_start_end = [
#         ('one_cpu_kafka_random', 1521793677 - 5, 1521793692 + 5),
#         ('two_cpu_kafka_random', 1521793705 - 5, 1521793720 + 5),
#         ('three_cpu_kafka_random', 1521793736 - 5, 1521793751 + 5),
#         ('four_cpu_kafka_random', 1521793768 - 5, 1521793783 + 5),
#         ('five_cpu_kafka_random', 1521793792 - 5, 1521793807 + 5),
#         ('six_cpu_kafka_random', 1521793831 - 5, 1521793846 + 5),
#         ('one_band_kafka_random', 1521793874 - 5, 1521793889 + 5),
#         ('two_band_kafka_random', 1521793953 - 5, 1521793968 + 5),
#         ('three_band_kafka_random', 1521794007 - 5, 1521794022 + 5),
#         ('four_band_kafka_random', 1521794068 - 5, 1521794083 + 5),
#         ('five_band_kafka_random', 1521794128 - 5, 1521794143 + 5),
#         ('six_band_kafka_random', 1521794221 - 5, 1521794236 + 5),
#         ('one_disk_kafka_random', 1521794273 - 5, 1521794288 + 5),
#         ('two_disk_kafka_random', 1521794322 - 5, 1521794337 + 5),
#         ('three_disk_kafka_random', 1521794348 - 5, 1521794363 + 5),
#         ('four_disk_kafka_random', 1521794376 - 5, 1521794391 + 5),
#         ('five_disk_kafka_random', 1521794405 - 5, 1521794420 + 5),
#         ('six_disk_kafka_random', 1521794434 - 5, 1521794449 + 5),
#         ('one_bigheap_kafka_random', 1521794468 - 5, 1521794483 + 5),
#         ('two_bigheap_kafka_random', 1521794512 - 5, 1521794527 + 5),
#         ('three_bigheap_kafka_random', 1521794560 - 5, 1521794575 + 5),
#         ('four_bigheap_kafka_random', 1521794615 - 5, 1521794630 + 5),
#         ('five_bigheap_kafka_random', 1521794680 - 5, 1521794695 + 5),
#         ('six_bigheap_kafka_random', 1521794722 - 5, 1521794737 + 5)
# ]
#     mongodb = 'localhost'
#     step = '1s'
#     # the folder where the results of the experiment are
#     experiments_res_path = '/Users/alvarobrandon/execo_experiments/gold_kafka_random/'
#     # the prometheus server from where we are going to get the metrics
#     prometheus_path = 'http://abrandon-vm.lille.grid5000.fr:9090/api/v1/query_range'
#     # the sysdig metrics can be found on the experiment folder
#     sysdig_path = experiments_res_path
#     # building the sysdig dataframe is a very expensive process. We build it once and then we use the index to select
#     # the data. Note how before we have a create_sysdig_df method that took start and end parameters to narrow the data
#     # size
#     # huge_sysdig_df = create_sysdig_df(sysdig_path)
#     # huge_sysdig_df.to_pickle(experiments_res_path + 'huge_sysdig_df.pickle')
#     huge_sysdig_df = pd.read_pickle(experiments_res_path + 'huge_sysdig_df.pickle')
#     prom_df = pd.read_pickle(experiments_res_path + 'huge_prom_df.pickle')
#     # the anomalies file is also on the experiment folder
#     anomalies_file = experiments_res_path + 'experiment_log.pickle'
#     for name, start, end in names_start_end:
#         # here we are going to dump the array of networX graphs that will be inserted into graph.timestamp
#         gephx_output_path = '/Users/alvarobrandon/RCAGephi/' + name + "/graph_sequence/"
#         if not exists(gephx_output_path):
#             makedirs(gephx_output_path)
#         # timestamp = 1507561419  # What happened this second?. What containers where active and what were their metrics?
#         # graph_sequence = load_gephx_sequence(output_path)
#         # graph_sequence = pickle.load(open(sysdig_path + 'graph_sequence.pickle', 'rb'))
#         # prom_df = create_prometheus_df(start, end, step, prometheus_path)
#         graph_sequence = build_graph_sequence(start, end, prom_df, huge_sysdig_df, anomalies_file,exec_profile)
#         create_gephx_sequence(graph_sequence, gephx_output_path)
#         mongodb_insert_graph_seq(mongodb, graph_sequence, name, name)
#         pickle.dump(graph_sequence, open(experiments_res_path + '{0}.pickle'.format(name), 'wb'))
#     # create_prometheus_df_backup(1521796996 - 10, 1521798384 + 10, step, prometheus_path,
#     #                             experiments_res_path + 'huge_prom_df.pickle')
#     print(exec_profile)
