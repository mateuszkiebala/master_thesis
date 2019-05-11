#!/usr/bin/env bash
# Author: Jacek Sroka
. ./settings.sh

#if you execute next lines for the first time they will only print errors
$SPARK_HOME/sbin/stop-all.sh
stop-yarn.sh
stop-dfs.sh

mkdir -p ${download_dir}

wget -nc -P ${download_dir} --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" https://download.oracle.com/otn-pub/java/jdk/8u202-b08/1961070e4c9b4e26a04e7f5a083f551e/jdk-8u202-linux-i586.tar.gz
wget -nc -P ${download_dir} http://archive.apache.org/dist/hadoop/common/hadoop-2.8.3/hadoop-2.8.3.tar.gz
wget -nc -P ${download_dir} https://archive.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz

tar -xvf ${download_dir}/jdk-8u202-linux-i586.tar.gz -C ${install_superdir}
tar -xvf ${download_dir}/hadoop-2.8.3.tar.gz -C ${install_superdir}
tar -xvf ${download_dir}/spark-2.3.0-bin-hadoop2.7.tgz -C ${install_superdir}

mkdir -p ${datanode_dir}
mkdir -p ${namenode_dir}


#you probably want to change this
master=$(hostname)
#changine this
cat <<EOF > ${etc_hadoop}/slaves
$(hostname)
EOF

sed -i -e "s|^export JAVA_HOME=\${JAVA_HOME}|export JAVA_HOME=$JAVA_HOME|g" ${etc_hadoop}/hadoop-env.sh
 
cat <<EOF > ${etc_hadoop}/core-site.xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${master}:9000</value>
    <description>NameNode URI</description>
  </property>
</configuration>
EOF
 
cat <<EOF > ${etc_hadoop}/hdfs-site.xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>3</value>
  </property>
 
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://${datanode_dir}</value>
    <description>Comma separated list of paths on the local filesystem of a DataNode where it should store its blocks.</description>
  </property>
 
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://${namenode_dir}</value>
    <description>Path on the local filesystem where the NameNode stores the namespace and transaction logs persistently.</description>
  </property>
 
  <property>
    <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
    <value>false</value>
    <description>http://log.rowanto.com/why-datanode-is-denied-communication-with-namenode/</description>
  </property>
</configuration>
EOF
 
 
cat <<EOF > ${etc_hadoop}/mapred-site.xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <!--
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>16384</value>
    </property>
    <property>
        <name>mapreduce.map.java.opts</name>
        <value>-Xmx15384m</value>
    </property>
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>16384</value>
    </property>
    <property>
        <name>mapreduce.reduce.java.opts</name>
        <value>-Xmx15384m</value>
    </property>
    -->
</configuration>
EOF
 
 
cat <<EOF > ${etc_hadoop}/yarn-site.xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>${master}</value>
   </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
   </property>
   <!--
   <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>25000</value>
   </property>
   <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>16384</value>
        <description>Max RAM-per-container https://stackoverflow.com/questions/43826703/difference-between-yarn-scheduler-maximum-allocation-mb-and-yarn-nodemanager</description>
   </property>
   -->
</configuration>
EOF


cat <<EOF > $SPARK_HOME/conf/spark-env.sh
#!/usr/bin/env bash

export JAVA_HOME=${JAVA_HOME}
export HADOOP_INSTALL=${HADOOP_INSTALL}
export HADOOP_PREFIX=${HADOOP_INSTALL}
export HADOOP_CONF_DIR=${etc_hadoop}
export SPARK_HOME=${SPARK_HOME}
export PATH=$JAVA_HOME/bin:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

export SPARK_WORKER_CORES=6
export SPARK_PUBLIC_DNS=${master}
EOF

cp ${etc_hadoop}/slaves $SPARK_HOME/conf/slaves 


while read name
do
  echo "============================== syncing to:" $name "==================================="
  
  ssh -n $user@$name rm -fr "$datanode_dir"
  ssh -n $user@$name rm -fr "$namenode_dir"
  ssh -n $user@$name mkdir $datanode_dir
  ssh -n $user@$name mkdir $namenode_dir
  rsync -zrvhae ssh $JAVA_HOME $user@$name:$install_superdir
  rsync -zrvhae ssh $HADOOP_INSTALL $user@$name:$install_superdir
  rsync -zrvhae ssh $SPARK_HOME $user@$name:$install_superdir
done < ${etc_hadoop}/slaves

hdfs namenode -format

start-dfs.sh
start-yarn.sh
sleep 5
hdfs dfsadmin -report
$SPARK_HOME/sbin/start-all.sh
#spark-shell --master spark://$(hostname):7077

