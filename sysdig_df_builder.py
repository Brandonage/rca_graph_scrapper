import subprocess
import pandas as pd
import io
import re
from os import listdir


# it will create a dataframe that is sliced in different dimensions the most important being time
# TODO: Revise how many dimensions do we need. I think we need all these dimensions in order to do the sum and count for
# each client, server, container.id, port????, host, container image and direction fo event
def create_sysdig_df(start, end, sysdig_path):
    scrape_files = [sysdig_path + f for f in listdir(sysdig_path) if 'scrap' in f]
    cmd_sysdig_filter = ['sysdig',
                         'evt.rawtime.s>{0} '.format(start),
                         'and',
                         'evt.rawtime.s<{0} '.format(end),
                         # 'and', we include writes as well. From each node we have an edge that has the attributes of how much it reads and how much it writes
                         # 'evt.io_dir=\"write\"',
                         '-p"%evt.rawtime.s,%fd.num,%fd.type,%evt.type,%evt.dir,%proc.name,%proc.pid,%container.name,%container.image,%container.id,%container.type,%fd.name,%fd.cip,%fd.sip,%fd.lip,%fd.rip,%fd.is_server,%fd.cport,%fd.sport,%fd.lport,%fd.rport,%fd.l4proto,%evt.io_dir,%evt.category,%evt.rawarg.res"',
                         '-r']
    # NOTE: we add the container.image but the idea is to take it out in the future since it filters out
    # processes that are not containers, like the zookeper of mesos and all of the native mesos processes
    # that run on the hosts

    # We print some information on what machines we are going to analyse
    for file_in in scrape_files:
        ip = re.findall(r'[0-9]+(?:\.[0-9]+){3}', file_in)
        print "Found files for the following machine: " + ip[0]

    # we put all the information of all the files on a buffer that we will use to build the DF
    csv = io.StringIO()

    for file_in in scrape_files:
        host = re.findall(r'[0-9]+(?:\.[0-9]+){3}', file_in)[0]
        # print cmd_sysdig_filter
        sysdig_process = subprocess.Popen(cmd_sysdig_filter + [file_in], stdout=subprocess.PIPE)
        for line in sysdig_process.stdout:
            csv.write(host + ',' + line.decode().strip('"\n') + '\n')
    csv.seek(0)
    data = pd.read_csv(csv,
                       index_col=1,
                       names=['evt.host', 'evt.rawtime.s', 'fd.num', 'fd.type', 'evt.type', 'evt.dir', 'proc.name',
                              'proc.pid', 'container.name', 'container.image', 'container.id', 'container.type',
                              'fd.name', 'fd.cip', 'fd.sip', 'fd.lip', 'fd.rip', 'fd.is_server', 'fd.cport', 'fd.sport',
                              'fd.lport', 'fd.rport', 'fd.l4proto', 'evt.io_dir', 'evt.category', 'evt.rawarg.res']
                       )
    csv.close()
    # we are going to group by some fields to get the relations between containers
    # we tried grouping by fd.num and fd.name. The problem with this is that the port is also included
    # a container can have several ports from where it can communicates. We have to decide if we are going
    # to have more than one edge between two nodes (most likely not so we should eliminate ports as a dimension)

    # we need to do an aggregation by sum and count. This creates new index that we are going to reset later on
    agg_df = data.groupby(['fd.cip',
                        'fd.sip',
                        'fd.lport',
                        'fd.rport',
                        'container.id',
                        'evt.host',
                        'evt.rawtime.s',
                        'container.image',
                        'evt.io_dir'])['evt.rawarg.res'].agg(['sum', 'count'])  # .reset_index() to have a DF
    return agg_df
