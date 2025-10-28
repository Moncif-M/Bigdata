#!/bin/bash

echo -e "\n"

#$HADOOP_HOME/sbin/start-dfs.sh
#$HADOOP_HOME/sbin/start-yarn.sh
echo -e "\n"
$HADOOP_HOME/sbin/start-all.sh
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

echo -e "\n"

