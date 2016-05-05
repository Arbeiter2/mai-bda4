import org.apache.hadoop.mapreduce.Partitioner;

public class CabIDTimestampPartitioner 
	extends  Partitioner<CabIDTimestamp, CabTripSegment> {

	@Override
    public int getPartition(CabIDTimestamp pair, 
                            CabTripSegment segment, 
                            int numberOfPartitions) {
    	// make sure that partitions are non-negative
        return Integer.parseInt(pair.getVehicleID().toString()) % numberOfPartitions;
    }

}
