CLASSPATH = .:bin:$(shell yarn classpath)
JAVAC = javac -cp $(CLASSPATH) -d bin


all: bin/Exercise1.jar bin/Exercise2.jar

clean:
	cd bin && rm -f *class Exercise1.jar Exercise2.jar

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
bin/CabIDTimestamp.class: src/CabIDTimestamp.java
	$(JAVAC) src/CabIDTimestamp.java
bin/CabIDCombiner.class: src/CabIDCombiner.java
	$(JAVAC) src/CabIDCombiner.java
bin/CabIDCombinerGroupComp.class: src/CabIDCombinerGroupComp.java
	$(JAVAC) src/CabIDCombinerGroupComp.java
bin/CabIDTimestampComp.class: src/CabIDTimestampComp.java
	$(JAVAC) src/CabIDTimestampComp.java
bin/CabIDTimestampSortComp.class: src/CabIDTimestampSortComp.java
	$(JAVAC) src/CabIDTimestampSortComp.java	
bin/CabIDTimestampPartitioner.class: src/CabIDTimestampPartitioner.java
	$(JAVAC) src/CabIDTimestampPartitioner.java
bin/CabTrips.class: src/CabTrips.java
	$(JAVAC) src/CabTrips.java

bin/CabTripRevenueRecord.class: src/CabTripRevenueRecord.java
	$(JAVAC) src/CabTripRevenueRecord.java
bin/TimezoneMapper.class: src/TimezoneMapper.java
	$(JAVAC) src/TimezoneMapper.java
bin/CabTripRevenueMapper.class: src/CabTripRevenueMapper.java
	$(JAVAC) src/CabTripRevenueMapper.java
bin/CabTripRevenueReducer.class: src/CabTripRevenueReducer.java
	$(JAVAC) src/CabTripRevenueReducer.java
bin/CabTripRevenueRecordComp.class: src/CabTripRevenueRecordComp.java
	$(JAVAC) src/CabTripRevenueRecordComp.java
bin/CabTripRevenueRecordPartitioner.class: src/CabTripRevenueRecordPartitioner.java
	$(JAVAC) src/CabTripRevenueRecordPartitioner.java
bin/CabTripRevenue.class: src/CabTripRevenue.java
	$(JAVAC) src/CabTripRevenue.java

bin/Exercise2.jar:	bin/CabTripSegment.class bin/CabIDTimestamp.class bin/CabIDTimestampComp.class \
	bin/CabIDTimestampPartitioner.class bin/GeoDistanceCalc.class bin/TimezoneMapper.class bin/CabIDTimestampSortComp.class \
	bin/CabTripMapper.class bin/CabIDCombinerGroupComp.class bin/CabIDCombiner.class bin/CabTripReducer.class \
	bin/CabTrips.class bin/CabTripRevenueRecord.class  bin/CabTripRevenueMapper.class bin/CabTripRevenueReducer.class \
	bin/CabTripRevenueRecordComp.class bin/CabTripRevenueRecordPartitioner.class  bin/CabTripRevenue.class 
	cd bin && jar cf Exercise2.jar GeoDistanceCalc.class GeoDistanceCalc*.class CabTripMapper.class CabTripMapper*.class \
		CabTripReducer.class CabTripReducer*.class CabIDCombinerGroupComp.class CabTrips.class CabTrips*.class CabTripRevenue.class CabTripRevenueMapper.class \
		CabIDTimestamp.class CabIDTimestampComp.class CabIDTimestampPartitioner.class CabIDTimestampSortComp.class \
		CabTripSegment.class CabTripRevenueReducer.class CabTripRevenueRecord.class CabTripRevenueRecordComp.class \
		CabTripRevenueRecordPartitioner.class TimezoneMapper.class TimezoneMapper*.class CabIDCombiner*.class
