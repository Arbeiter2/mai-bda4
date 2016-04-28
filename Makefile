CLASSPATH = .:/usr/lib/jvm/default-java/lib:/home/u0098478/cluster1_conf:/home/u0098478/cluster1_conf:/home/u0098478/cluster1_conf:/home/u0098478/hadoop-2.6.0/share/hadoop/common/lib/*:/home/u0098478/hadoop-2.6.0/share/hadoop/common/*:/home/u0098478/hadoop-2.6.0/share/hadoop/hdfs:/home/u0098478/hadoop-2.6.0/share/hadoop/hdfs/lib/*:/home/u0098478/hadoop-2.6.0/share/hadoop/hdfs/*:/home/u0098478/hadoop-2.6.0/share/hadoop/yarn/lib/*:/home/u0098478/hadoop-2.6.0/share/hadoop/yarn/*:/home/u0098478/hadoop-2.6.0/share/hadoop/mapreduce/lib/*:/home/u0098478/hadoop-2.6.0/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:/home/u0098478/hadoop-2.6.0/share/hadoop/yarn/*:/home/u0098478/hadoop-2.6.0/share/hadoop/yarn/lib/*
JAVAC = javac -cp $(CLASSPATH)

all: Exercise1.jar Exercise2.jar

CabTripDist.class:	CabTripDist.java
	$(JAVAC) CabTripDist.java
GeoDistanceCalc.class:	GeoDistanceCalc.java
	$(JAVAC) GeoDistanceCalc.java
Exercise1.jar:	CabTripDist.class GeoDistanceCalc.class
	jar cf Exercise1.jar CabTripDist*.class GeoDistanceCalc*.class

CabTripMapper.class:	CabTripMapper.java
	$(JAVAC) CabTripMapper.java
CabTripReducer.class:	CabTripReducer.java
	$(JAVAC) CabTripReducer.java
CabTripSegment.class:	CabTripSegment.java
	$(JAVAC) CabTripSegment.java
VehicleIDTimestamp.class:	VehicleIDTimestamp.java
	$(JAVAC) VehicleIDTimestamp.java
VehicleIDTimestampComparator.class:	VehicleIDTimestampComparator.java
	$(JAVAC) VehicleIDTimestampComparator.java
VehicleIDTimestampPartitioner.class:	VehicleIDTimestampPartitioner.java
	$(JAVAC) VehicleIDTimestampPartitioner.java
CabTrips.class:	CabTrips.java
	$(JAVAC) CabTrips.java

Exercise2.jar:	CabTripDist.class GeoDistanceCalc.class
	jar cf Exercise2.jar GeoDistanceCalc*.class CabTripMapper*.class \
		CabTripReducer*.class CabTripSegment*.class CabTrips*.class \
		VehicleIDTimestamp*.class VehicleIDTimestampComparator*.class VehicleIDTimestampPartitioner*.class
