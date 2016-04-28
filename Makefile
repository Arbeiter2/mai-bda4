CLASSPATH=/usr/local/hadoop-2.7.2/etc/hadoop:/usr/local/hadoop-2.7.2/etc/hadoop:/usr/local/hadoop-2.7.2/etc/hadoop:/usr/local/hadoop-2.7.2/share/hadoop/common/lib/*:/usr/local/hadoop-2.7.2/share/hadoop/common/*:/usr/local/hadoop-2.7.2/share/hadoop/hdfs:/usr/local/hadoop-2.7.2/share/hadoop/hdfs/lib/*:/usr/local/hadoop-2.7.2/share/hadoop/hdfs/*:/usr/local/hadoop-2.7.2/share/hadoop/yarn/lib/*:/usr/local/hadoop-2.7.2/share/hadoop/yarn/*:/usr/local/hadoop-2.7.2/share/hadoop/mapreduce/lib/*:/usr/local/hadoop-2.7.2/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:/usr/local/hadoop-2.7.2/share/hadoop/yarn/*:/usr/local/hadoop-2.7.2/share/hadoop/yarn/lib/*

all:
	javac -cp ${CLASSPATH} *.java 
	jar cf Exercise1.jar *.class
