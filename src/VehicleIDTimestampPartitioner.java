import org.apache.hadoop.mapreduce.Partitioner;

public class VehicleIDTimestampPartitioner 
	extends  Partitioner<VehicleIDTimestamp, CabTripSegment> {

	@Override
    public int getPartition(VehicleIDTimestamp pair, 
                            CabTripSegment segment, 
                            int numberOfPartitions) {
    	// make sure that partitions are non-negative
        return Integer.parseInt(pair.getVehicleID().toString()) % numberOfPartitions;
    }

}
