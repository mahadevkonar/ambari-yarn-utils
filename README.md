ambari-yarn-utils
=================

Ambari YARN UTILS.

Use the yarn-utils script to calculate the various parameters for memory in YARN
and MapReduce 2. 

./yarn-utils --help for more info.

Example:

yarn-utils.py -c 16 -m 64 -d 4 -k True


by default the script uses the following:

cores = 16 (-c option)
memory = 64 (-m option)
disks = 4 (-d option)
hbaseEnabled (-k option)
