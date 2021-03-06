In this repository there is sample data to try the graph builder. 

The outputs of an execution of the graph_builder.py script are contained in the outputs subdirectory.  

#### How to get your own sysdig dumps.

Here we provide some sample sysdig dumps (10.136.41.*.scrap.gz files). In case you want to generate your own in your testbed you should launch sysdig as a container with the following arguments

`sudo docker run -i -t --name sysdig --privileged -v /var/run/docker.sock:/host/var/run/docker.sock -v /dev:/host/dev -v /proc:/host/proc:ro -v /boot:/host/boot:ro -v /lib/modules:/host/lib/modules:ro -v /usr:/host/usr:ro -v /home/vagrant:/host/vagrant sysdig/sysdig sysdig "not(proc.name contains stress-ng)" and "(fd.type=ipv4 or fd.type=ipv6)" and evt.is_io=true -p"%evt.rawtime.s,%fd.num,%fd.type,%evt.type,%evt.dir,%proc.name,%proc.pid,%container.name,%container.image,%container.id,%container.type,%fd.name,%fd.cip,%fd.sip,%fd.lip,%fd.rip,%fd.is_server,%fd.cport,%fd.sport,%fd.lport,%fd.rport,%fd.l4proto,%evt.io_dir,%evt.category,%evt.rawarg.res" > $HOME/{{{host}}}.scrap'`     

This will monitor all the network communication between containers and hosts and dump it into a text file. The `not(proc.name contains stress-ng)` is specific to our use case since we did not want to monitor any traffic generated by the stress-ng process. This can be changed depending on your use case  