from prometheus_df_builder import create_prometheus_df
from sysdig_df_builder import create_sysdig_df
import networkx as nx
from os import makedirs, listdir, remove
from os.path import exists, expanduser
from mongo_exporter import mongodb_insert_graph_seq


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


def create_prometheus_node(container_id,element_info,graph):
    """
    Create a node that is inserted into the graph with the information contained in a DF that has two indexes:
    metric_type with the type of metric and host with the host where this metric was scraped from
    :param element_info: we get data through inplugin.collect() every coll_period secs
    :type element_info: DataFrame
    """
    for metric in element_info.iterrows(): # I have a node to create
        # metric is a tuple that has as the first element the remaining index elements. Note how we eliminated the time
        # dimension since this is a snapshot, and we also eliminated the id dimension since we are slicing for each.
        # the two dimensions left are the type of metric and the host.
        # The second element of the tuple is a pd.series. It has the value for the type of metric and a dict with information
        # inserted by the scrapper for that metric
        graph.add_node(container_id,attr_dict={metric[0][0] : optional_float(metric[1][0])}) # the metric cloud be a string or float
        # some metrics have specific information about that container or element id in a dict. Things like the name or the
        # mesos task ID. This is contained in metric[1][1] and we are going to store that in the node as well. Note how usually these are repeated for the
        # same scrapper. For example cadvisor is always going to include mesos task ID if the container was launched
        # in Mesos. One thing we could do could be grouping by metric column but it's not hashable and so, we just call
        # the add_attribute function for each dict
        graph.add_node(container_id,attr_dict=metric[1][1])
        graph.add_edge(container_id,metric[0][1]) # and we also add an edge between the element and the host that contains it.

def add_prometheus_information(graph,prom_snapshot):
    for container_id in prom_snapshot.index.get_level_values("id").values: # for each id in the index
        create_prometheus_node(container_id,element_info=prom_snapshot.xs(container_id,level="id"),graph=graph) # I take its information and I create a node in the graph

def add_sysdig_information(graph,sysdig_snapshot):
    for idx, df_comm in sysdig_snapshot.reset_index(level=range(2, sysdig_snapshot.index.names.__len__())).groupby(level=[0, 1]):
        if df_comm.shape[0] == 2:  # if there is only one node in our monitred data... (this will possibly be avoided when we include the native host processes)
            bytes_read, bytes_write = tuple(df_comm['sum'].values)
            req_read, req_write = tuple(df_comm['count'].values)
            # the destination of the edge is just the machine it communicates with, that is idx[1]
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
    return DG


def build_graph_sequence(start,end,step,prometheus_path,sysdig_path):
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
    return graph_sequence


if __name__ == '__main__':
    name = "First experiment"
    mongodb = '10.130.224.163'
    folder_name = "rcavagrant__03_Nov_2017_16:25/"
    output_path = expanduser("~") + "/rca_graphs/" + folder_name
    if not exists(output_path):
        makedirs(output_path)
    start = 1509724046 - 60
    end = 1509724046 + 60
    step = '1s'
    prometheus_path = 'http://fnancy.nancy.grid5000.fr:9090/api/v1/query_range'
    sysdig_path = '/Users/alvarobrandon/execo_experiments/rcavagrant__03_Nov_2017_16:25/'
    # timestamp = 1507561419  # What happened this second?. What containers where active and what were their metrics?
    # graph_sequence = load_gephx_sequence(output_path)
    graph_sequence = build_graph_sequence(start,end,step,prometheus_path,sysdig_path)
    # create_gephx_sequence(graph_sequence,output_path)
    mongodb_insert_graph_seq(mongodb,graph_sequence,'first_experiment',name)
