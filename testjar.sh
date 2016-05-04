#!/bin/sh

PREFIX=/tmp/trips.706
LIST=""
for F_VALUE in c s
do
	for D_VALUE in e h
	do
		OUT=${PREFIX}.${F_VALUE}${D_VALUE}
		LIST="${F_VALUE}${D_VALUE} ${LIST}"
		hadoop fs -rm -r ${OUT}
		hadoop jar bin/Exercise2.jar CabTrips -i /tmp/taxi_706.unsorted -o ${OUT} -f ${F_VALUE} -d ${D_VALUE} 2> /tmp/trips.${F_VALUE}${D_VALUE}.errs 1> /tmp/trips.${F_VALUE}${D_VALUE}.logs
	done
done

for S_VALUE in "" -s
do
	for D_VALUE in e h
	do
		for MODE in $LIST
		do
			IN=${PREFIX}.${MODE}
			OUT=${PREFIX}.revenue.${MODE}-${S_VALUE#-}${D_VALUE}
			hadoop fs -rm -r ${OUT}
			hadoop jar bin/Exercise2.jar CabTripRevenue -i ${IN} -o ${OUT} -C 3.50,1.71 -L SFO,37.62131,-122.37896,1.00 ${S_VALUE} -d ${D_VALUE} 2> /tmp/revenue.${MODE}-${S_VALUE#-}${D_VALUE}.errs 1> /tmp/revenue.${MODE}-${S_VALUE#-}${D_VALUE}.logs
		done
	done
done
