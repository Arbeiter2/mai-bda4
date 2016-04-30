import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class VehicleIDTimestampPartitioner 
	extends  Partitioner<VehicleIDTimestamp, CabTripSegment> {

	@Override
    public int getPartition(VehicleIDTimestamp pair, 
                            CabTripSegment segment, 
                            int numberOfPartitions) {
    	// make sure that partitions are non-negative
        return Math.abs(pair.getVehicleID().hashCode() % numberOfPartitions);
    }

}
