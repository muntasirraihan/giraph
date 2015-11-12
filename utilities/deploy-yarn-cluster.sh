#!/bin/bash

#cd /home/muntasir/hadoop_natjam_deadline/hadoop-common
#git pull
#mvn3 package -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true
#cd /home/muntasir/hadoop_natjam_deadline/hadoop-common/hadoop-dist/target
#sudo scp hadoop-0.23.3-SNAPSHOT.tar.gz mrahman2@node-00.natjam-test.iss.emulab.net:/proj/ISS/scheduling

EXPECTED_ARGS=2

if [ $# -ne $EXPECTED_ARGS ]
then
    echo "Usage: ./run_natjam_emulab.sh <num_nodes> <clustername>"
    echo "cluster_name = pcap-cluster"
    exit 2
fi


NUM_NODES=$1
CLUSTER_NAME=$2
START_NODE=0
MASTER_NODE=0

HEAP_SIZE=2048

HADOOP_NAME=hadoop-2.7.0

rm -f /proj/ISS/scheduling/slaves
for (( i = $START_NODE + 1; i < $NUM_NODES; i++ ))
do
    echo "node-0$i.$CLUSTER_NAME.iss.emulab.net">> /proj/ISS/scheduling/slaves
done

JAVA_COMMAND="sudo apt-get install -y openjdk-7-jdk"

#for (( c = 2; c < $NUM_NODES; c++ ))
#do
#    ssh node-0$c $JAVA_COMMAND
#done

STOP_COMMAND="killall -9 java"

for (( i = 0; i < $NUM_NODES; i++ ))
do
    ssh node-0$i $STOP_COMMAND
done


COMMAND="rm -rf /mnt/hadoop; mkdir -p /mnt/hadoop; chmod a+w /mnt/hadoop; cp /proj/ISS/scheduling/$HADOOP_NAME.tar.gz /mnt/hadoop; tar xzf /mnt/hadoop/$HADOOP_NAME.tar.gz -C /mnt/hadoop; mkdir -p /mnt/hadoop/$HADOOP_NAME/conf; chmod -R 777 /mnt/hadoop/$HADOOP_NAME/conf; cp -r /proj/ISS/scheduling/conf/* /mnt/hadoop/$HADOOP_NAME/conf; rm -rf /mnt/hadoop/$HADOOP_NAME/conf/slaves; cp /proj/ISS/scheduling/slaves /mnt/hadoop/$HADOOP_NAME/conf/; rm -rf /tmp/mrahman2; mkdir -p /tmp/mrahman2; chmod -R 777 /tmp/mrahman2; mkdir /tmp/mrahman2/$HADOOP_NAME; chmod -R 777 /tmp/mrahman2/$HADOOP_NAME; mkdir /tmp/mrahman2/$HADOOP_NAME/nm-local-dirs/; chmod -R 777 /tmp/mrahman2/$HADOOP_NAME/nm-local-dirs/; mkdir /tmp/mrahman2/$HADOOP_NAME/nm-log-dirs/; chmod -R 777 /tmp/mrahman2/$HADOOP_NAME/nm-log-dirs/" 	

for (( c = $START_NODE; c < $NUM_NODES; c++ ))
do
    ssh node-0$c "echo \"extracting on $c\"; $COMMAND" &
done

# wait for all extraction commands to finish
for job in $(jobs -p); do
  wait $job
done

HADOOP_MASTER_COMMAND_OLD="JAVA_HOME=/usr; export JAVA_HOME; YARN_RESOURCEMANAGER_HEAPSIZE=$HEAP_SIZE; export YARN_RESOURCEMANAGER_HEAPSIZE; cd /mnt/hadoop/hadoop-0.23.3-SNAPSHOT/; PATH=$PWD/bin:$PATH; export PATH; PATH=$PWD/sbin:$PATH; export PATH; HADOOP_CONF_DIR=$PWD/conf; export HADOOP_CONF_DIR; ./hdfs namenode -format; ./hadoop-daemon.sh start namenode; ./yarn-daemon.sh start resourcemanager"

HADOOP_MASTER_COMMAND="JAVA_HOME=/usr; export JAVA_HOME; YARN_RESOURCEMANAGER_HEAPSIZE=$HEAP_SIZE; export YARN_RESOURCEMANAGER_HEAPSIZE; HADOOP_CONF_DIR=/mnt/hadoop/$HADOOP_NAME/conf; export HADOOP_CONF_DIR; cd /mnt/hadoop/$HADOOP_NAME/bin/; ./hdfs namenode -format; cd /mnt/hadoop/$HADOOP_NAME/sbin/; ./hadoop-daemon.sh stop namenode; ./hadoop-daemon.sh start namenode; ./yarn-daemon.sh stop resourcemanager; ./yarn-daemon.sh start resourcemanager; ./mr-jobhistory-daemon.sh stop historyserver; ./mr-jobhistory-daemon.sh start historyserver"

ssh node-0$MASTER_NODE $HADOOP_MASTER_COMMAND

HADOOP_SLAVE_COMMAND_OLD="JAVA_HOME=/usr; export JAVA_HOME; cd /mnt/hadoop/hadoop-0.23.3-SNAPSHOT/; PATH=$PWD/bin:$PATH; export PATH; PATH=$PW\
D/sbin:$PATH; export PATH; HADOOP_CONF_DIR=$PWD/conf; export HADOOP_CONF_DIR; ./hdfs namenode -format; ./hadoop-daemon.sh start namenode; ./yarn-daemon.sh start resourcemanager"

HADOOP_SLAVE_COMMAND="JAVA_HOME=/usr; export JAVA_HOME; YARN_RESOURCEMANAGER_HEAPSIZE=$HEAP_SIZE; export YARN_RESOURCEMANAGER_HEAPSIZE; HADOOP_CONF_DIR=/mnt/hadoop/$HADOOP_NAME/conf; export HADOOP_CONF_DIR; cd /mnt/hadoop/$HADOOP_NAME/sbin/; ./hadoop-daemon.sh stop datanode; ./hadoop-daemon.sh start datanode; ./yarn-daemon.sh stop nodemanager; ./yarn-daemon.sh start nodemanager"

for (( i = 1; i < $NUM_NODES; i++ ))
do
    ssh node-0$i $HADOOP_SLAVE_COMMAND &
done

# wait for each slave to be up before exiting
for job in $(jobs -p); do
  wait $job
done
echo "$CLUSTER_NAME is now up; remember to prepare input!"
