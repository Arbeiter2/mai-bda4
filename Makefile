CLASSPATH = .:bin:$(shell yarn classpath)
JAVAC = javac -cp $(CLASSPATH) -d bin


all: Exercise1.jar Exercise2.jar

clean:
	rm *class Exercise1.jar Exercise2.jar

GeoDistanceCalc.class: src/GeoDistanceCalc.java
	$(JAVAC) src/GeoDistanceCalc.java
CabTripDist.class: src/CabTripDist.java
	$(JAVAC) src/CabTripDist.java
Exercise1.jar:	GeoDistanceCalc.class CabTripDist.class
	cd bin && jar cf Exercise1.jar CabTripDist.class CabTripDist*.class GeoDistanceCalc.class

CabTripSegment.class: src/CabTripSegment.java
	$(JAVAC) src/CabTripSegment.java
CabTripMapper.class: src/CabTripMapper.java
	$(JAVAC) src/CabTripMapper.java
CabTripReducer.class: src/CabTripReducer.java
	$(JAVAC) src/CabTripReducer.java
VehicleIDTimestamp.class: src/VehicleIDTimestamp.java
	$(JAVAC) src/VehicleIDTimestamp.java
VehicleIDTimestampComparator.class: src/VehicleIDTimestampComparator.java
	$(JAVAC) src/VehicleIDTimestampComparator.java
VehicleIDTimestampPartitioner.class: src/VehicleIDTimestampPartitioner.java
	$(JAVAC) src/VehicleIDTimestampPartitioner.java
CabTrips.class: src/CabTrips.java
	$(JAVAC) src/CabTrips.java

Exercise2.jar:	VehicleIDTimestamp.class VehicleIDTimestampComparator.class VehicleIDTimestampPartitioner.class GeoDistanceCalc.class CabTripSegment.class CabTripMapper.class CabTripReducer.class CabTrips.class
	cd bin && jar cf Exercise2.jar GeoDistanceCalc.class GeoDistanceCalc*.class CabTripMapper.class CabTripMapper*.class \
		CabTripReducer.class CabTripReducer*.class CabTrips.class CabTrips*.class \
		VehicleIDTimestamp.class VehicleIDTimestampComparator.class VehicleIDTimestampPartitioner.class CabTripSegment.class
