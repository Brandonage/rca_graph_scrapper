import requests
import pandas as pd
import re
# we have two options to distinguish between metrics. In the monitoring job of prometheus, use labels to differentiate between container metrics
# and host metrics with the dict_container['metric]['job'] data e.g. containers_and_hosts.
# or do it here with the metrics separated in two differents arrays



def michals_avg_std(start,end,step,prometheus_url):
    df = create_prometheus_df(start,end,step,prometheus_url)
    scenario = "cassandra"
    dataframe_mean = []
    dataframe_std = []
    for container_id in df.index.levels[0].values:
        row_mean = {}
        row_std = {}
        row_mean.update({'container_id' : container_id})
        row_std.update({'container_id' : container_id})
        for metric in df.xs(container_id,level="id").reset_index(level=0)['metric_type'].unique():
            mean = df.xs((container_id, metric), level=["id", "metric_type"])['value'].astype(float).mean()
            std = df.xs((container_id, metric), level=["id", "metric_type"])['value'].astype(float).std()
            row_mean.update({metric : mean})
            row_std.update({metric : std})
        dataframe_mean.append(row_mean)
        dataframe_std.append(row_std)
    df_mean = pd.DataFrame(dataframe_mean)
    df_std = pd.DataFrame(dataframe_std)
    df_mean.to_excel('/Users/alvarobrandon/Desktop/{0}_stats_mean.xls'.format(scenario))
    df_std.to_excel('/Users/alvarobrandon/Desktop/{0}_stats_std.xls'.format(scenario))

# This function is going to build a dataframe from a prometheus endpoint with the metrics of the containers and hosts
# sliced in three dimensions: time, metric, container_id and host
def create_prometheus_df(start,end,step,prometheus_url):
    range = int(end) - int(start)
    rate = int(step.replace('s', '')) + 5
    # we use the name=~\".+\" part to make sure that we only get containers. We will have to change this if we want to get
    # also the processes.
    container_query_and_metric = [
        ("rate(container_cpu_user_seconds_total{name=~\".+\"}[" + str(rate) + "s])*100",'cpu_usr_cont'),
        ("rate(container_cpu_system_seconds_total{name=~\".+\"}[" + str(rate) + "s])*100",'cpu_sys_cont'),
        ("rate(container_cpu_cfs_throttled_seconds_total{name=~\".+\"}[" + str(rate) + "s])*100", 'cpu_wait_cont'),
        ("rate(container_network_receive_bytes_total{name=~\".+\",interface=\"eth0\"}[" + str(rate) + "s])",'net_recv'),
        ("rate(container_network_transmit_bytes_total{name=~\".+\",interface=\"eth0\"}[" + str(rate) + "s])",'net_sent'),
        ("rate(container_network_receive_packets_total{name=~\".+\",interface=\"eth0\"}[" + str(rate) + "s])",'packet_recv'),
        ("rate(container_network_transmit_packets_total{name=~\".+\",interface=\"eth0\"}[" + str(rate) + "s])",'packet_sent'),
        ("container_memory_usage_bytes{name=~\".+\"}",'mem_usage'),
        ("container_memory_cache{name=~\".+\"}", 'mem_cache_cont'),
        ("rate(container_memory_failures_total{name=~\".+\",scope=\"container\",type=\"pgfault\"}[" + str(rate) + "s])","pg_fault"),
        ("rate(container_memory_failures_total{name=~\".+\",scope=\"container\",type=\"pgmajfault\"}[" + str(rate) + "s])", "pgmaj_fault")
    ]
    host_query_and_metrics = [
        ("avg without(cpu)(rate(node_cpu{mode=\"user\"}[" + str(rate) + "s]) * 100)",'cpu_usr'),
        ("avg without(cpu)(rate(node_cpu{mode=\"system\"}[" + str(rate) + "s]) * 100)",'cpu_sys'),
        ("avg without(cpu)(rate(node_cpu{mode=\"iowait\"}[" + str(rate) + "s]) * 100)",'cpu_wait'),
        ("rate(node_context_switches[" + str(rate) + "s])",'ctx_switch'),
        ("rate(node_forks[" + str(rate) + "s])", 'node_forks'),
        ("rate(node_vmstat_pgalloc_normal[" + str(rate) + "s])", 'vm_alloc'),
        ("rate(node_vmstat_pgfault[" + str(rate) + "s])", 'vm_fault'),
        ("rate(node_vmstat_pgfree[" + str(rate) + "s])", 'vm_free'),
        ("sum without(device)(rate(node_disk_bytes_read[" + str(rate) + "s]))",'disk_read'),
        ("sum without(device)(rate(node_disk_bytes_written[" + str(rate) + "s]))",'disk_written'),
        ("sum(node_filesystem_free) without (device,fstype,mountpoint)",'fs_free'),
        ("node_memory_MemFree",'mem_free'),
        ("node_memory_MemTotal",'mem_total'),
        ("node_memory_Mlocked", "mem_locked"),
        ("node_memory_Cached", "mem_cache"),
        ("node_sockstat_sockets_used","sockets_used"),
        ("node_procs_running","procs_run"),
        ("rate(node_network_transmit_packets{device=\"eth0\"}[" + str(rate) + "s])","packet_sent"),
        ("rate(node_network_receive_packets{device=\"eth0\"}[" + str(rate) + "s])", "packet_recv"),
        ("rate(node_network_transmit_bytes{device=\"eth0\"}[" + str(rate) + "s])", "net_sent"),
        ("rate(node_network_receive_bytes{device=\"eth0\"}[" + str(rate) + "s])", "net_recv")
    ]
    list_of_df = []
    for query, metric in container_query_and_metric + host_query_and_metrics:
        parameters = {"query": query, "start": start, "end": end, "step": step}
        r = requests.get(prometheus_url, params=parameters).json()
        for dict_container in r['data']['result']: # ['data']['result'] is a list of dictionaries with two fields.
            # The metric information which is another dict like
            # {u'job': u'cont_and_hosts', u'instance': u'10.158.13.95:8082', u'image': u'google/cadvisor:latest',
            # u'id': u'/docker/0fd46bbdb15b90194a77970e536b112680826a17af5d0867594d6a5125c9e807', u'name': u'cadvisor'}
            # and the values, which is a list of pairs with the timestamp and the value of the metric
            scraped_host = re.findall(r'[0-9]+(?:\.[0-9]+){3}',dict_container['metric']['instance'])[0]  # we are going to index/group by metric(above), host and container_id
            if (query,metric) in container_query_and_metric: # if the metric is a container metric we consider the id to be the short Docker ID
                container_id = dict_container['metric'].get('id')[8:20]
            if (query,metric) in host_query_and_metrics:
                container_id = scraped_host
            len_time_series = len(dict_container[
                                      'values'])  
            # For the dict_container we are currently processing the metric, the host and the contianer id is going to
            # be repeated across rows. We leverage this by repeating the values by the number of rows and using it as
            # an index. These three dimensions together with the time dimensions will allow us to slice our data
            array_indexes = pd.MultiIndex.from_arrays(
                [[container_id] * len_time_series,
                 [metric] * len_time_series,
                 [scraped_host] * len_time_series],
                names=("id", "metric_type", "host")
            )
            # besides the pair timestamp, value we also want to include additional info like the image of the container
            # or the id. To do that we append the information that is contained on the metric part of dict_container
            [ts_point.append(dict_container['metric']) for ts_point in dict_container['values']]
            # we build a dataframe with rows that are going to container, the time the metric was recorded, the value
            # and a dictionary (metric) which is going to have additional info.
            df = pd.DataFrame(dict_container['values'], columns=['time', 'value', 'metric'], index=array_indexes)
            # we append it to a list of dataframes. Each dataframe is going to be the values of one metric for one
            # container during the time range specified. We then concat this list of DF's and pandas does all the
            # heavylifting of merging the indexes of the different DF's.
            list_of_df.append(df)
    return pd.concat(list_of_df).set_index(keys='time',append=True) # the last dimension we are going to use to slice is time