# From: https://www.baeldung.com/linux/use-command-line-arguments-in-bash-script
while getopts n flag
do
    case "${flag}" in
        n) isNamenode=1;;
    esac
done

if [[ $isNamenode == 1 ]]; then
# We're a namenode
echo "Instance is running as a namenode"
else
# We're a datanode
echo "Instance is running as a datanode"
fi

cd

#sudo apt-get -q update && sudo apt-get -q install -y unzip python3 python3-pip openjdk-8-jdk-headless
#pip3 install mrjob

# Upload with scp instead, much MUCH faster holy moly these vm's have limited internet connections
#wget --no-verbose -O hadoop-3.3.1.tar.gz "https://distributed.blob.core.windows.net/public/hadoop-3.3.1.tar.gz?sv=2020-10-02&st=2022-01-26T11%3A21%3A31Z&se=2022-08-27T10%3A21%3A00Z&sr=b&sp=r&sig=yqHR7Ohcs6OK8huoA9Rlqm7ciCK0sJd5hsVvfcQBoso%3D"
tar -xzf hadoop-3.3.1.tar.gz

sudo mv hadoop-3.3.1 /usr/local/hadoop
sudo tee /etc/environment > /dev/null << EOL
PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/usr/local/hadoop/bin:/usr/local/hadoop/sbin"
JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
HADOOP_HOME="/usr/local/hadoop"
EOL

# On all nodes
source /etc/environment

# On all nodes
#sudo tee -a /etc/hosts > /dev/null << EOL
#10.10.1.206 namenode
#10.10.1.150 datanode1
#10.10.1.9 datanode2
#10.10.1.27 datanode3
#10.10.1.220 datanode4
#EOL

# On all nodes
sudo tee /usr/local/hadoop/etc/hadoop/hdfs-site.xml > /dev/null << EOL
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/usr/local/hadoop/data/nameNode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/usr/local/hadoop/data/dataNode</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
</configuration>
EOL

# On all nodes
sudo tee /usr/local/hadoop/etc/hadoop/core-site.xml > /dev/null << EOL
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://namenode:9000</value>
  </property>
</configuration>
EOL

# On all nodes b
sudo tee /usr/local/hadoop/etc/hadoop/yarn-site.xml > /dev/null << EOL
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
      <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
      <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>namenode</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
</configuration>
EOL

# On all nodes b
# See also for last property: https://stackoverflow.com/questions/50927577/could-not-find-or-load-main-class-org-apache-hadoop-mapreduce-v2-app-mrappmaster
sudo tee /usr/local/hadoop/etc/hadoop/mapred-site.xml > /dev/null << EOL
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
    </property>
    <property> 
      <name>mapreduce.application.classpath</name>
      <value>$HADOOP_HOME/share/hadoop/mapreduce/*,$HADOOP_HOME/share/hadoop/mapreduce/lib/*,$HADOOP_HOME/share/hadoop/common/*,$HADOOP_HOME/share/hadoop/common/lib/*,$HADOOP_HOME/share/hadoop/yarn/*,$HADOOP_HOME/share/hadoop/yarn/lib/*,$HADOOP_HOME/share/hadoop/hdfs/*,$HADOOP_HOME/share/hadoop/hdfs/lib/*</value>
    </property>
</configuration>
EOL

# On all nodes
sudo tee /usr/local/hadoop/etc/hadoop/workers > /dev/null << EOL
datanode1
datanode2
datanode3
datanode4
EOL

# On all nodes
sudo tee /usr/local/hadoop/etc/hadoop/masters > /dev/null << EOL
namenode
EOL

if [[ $isNamenode == 1 ]]; then
cd ~/.ssh/
ssh-keygen -t ed25519 -f unikey -q -N ""
eval $(ssh-agent)
ssh-add unikey
fi