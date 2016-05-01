CLASSPATH = .:bin:$(shell yarn classpath)
JAVAC = javac -cp $(CLASSPATH) -d bin


all: bin/Exercise1.jar bin/Exercise2.jar

clean:
	cd bin && rm *class Exercise1.jar Exercise2.jar

bin/GeoDistanceCalc.class: src/GeoDistanceCalc.java
	$(JAVAC) src/GeoDistanceCalc.java
bin/CabTripDist.class: src/CabTripDist.java
	$(JAVAC) src/CabTripDist.java
bin/Exercise1.jar:	bin/GeoDistanceCalc.class bin/CabTripDist.class
	cd bin && jar cf Exercise1.jar CabTripDist.class CabTripDist*.class GeoDistanceCalc.class

bin/CabTripSegment.class: src/CabTripSegment.java
	$(JAVAC) src/CabTripSegment.java
bin/CabTripMapper.class: src/CabTripMapper.java
	$(JAVAC) src/CabTripMapper.java
bin/CabTripReducer.class: src/CabTripReducer.java
	$(JAVAC) src/CabTripReducer.java
bin/VehicleIDTimestamp.class: src/VehicleIDTimestamp.java
	$(JAVAC) src/VehicleIDTimestamp.java
bin/VehicleIDTimestampComparator.class: src/VehicleIDTimestampComparator.java
	$(JAVAC) src/VehicleIDTimestampComparator.java
bin/VehicleIDTimestampPartitioner.class: src/VehicleIDTimestampPartitioner.java
	$(JAVAC) src/VehicleIDTimestampPartitioner.java
bin/CabTrips.class: src/CabTrips.java
	$(JAVAC) src/CabTrips.java

bin/CabTripCostRecord.class: src/CabTripCostRecord.java
	$(JAVAC) src/CabTripCostRecord.java
bin/CabTripCostMapper.class: src/CabTripCostMapper.java
	$(JAVAC) src/CabTripCostMapper.java
bin/CabTripCostReducer.class: src/CabTripCostReducer.java
	$(JAVAC) src/CabTripCostReducer.java
bin/CabTripCostRecordComparator.class: src/CabTripCostRecordComparator.java
	$(JAVAC) src/CabTripCostRecordComparator.java
bin/CabTripCostRecordPartitioner.class: src/CabTripCostRecordPartitioner.java
	$(JAVAC) src/CabTripCostRecordPartitioner.java
bin/CabTripCost.class: src/CabTripCost.java
	$(JAVAC) src/CabTripCost.java

bin/Exercise2.jar:	bin/VehicleIDTimestamp.class bin/VehicleIDTimestampComparator.class bin/CabTripCostReducer.class \
	bin/VehicleIDTimestampPartitioner.class bin/GeoDistanceCalc.class bin/CabTripSegment.class \
	bin/CabTripMapper.class bin/CabTripReducer.class bin/CabTrips.class  bin/CabTripCostRecord.class bin/CabTripCostMapper.class bin/CabTripCostReducer.class bin/CabTripCostRecordComparator.class \
	bin/CabTripCostRecordPartitioner.class  bin/CabTripCost.class
	cd bin && jar cf Exercise2.jar GeoDistanceCalc.class GeoDistanceCalc*.class CabTripMapper.class CabTripMapper*.class \
		CabTripReducer.class CabTripReducer*.class CabTrips.class CabTrips*.class CabTripCost.class CabTripCostMapper.class \
		VehicleIDTimestamp.class VehicleIDTimestampComparator.class VehicleIDTimestampPartitioner.class \
		CabTripSegment.class CabTripCostReducer.class CabTripCostRecord.class CabTripCostRecordComparator.class \
		CabTripCostRecordPartitioner.class
