import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class VehicleIDTimestampPartitioner 
	extends  Partitioner<VehicleIDTimestamp, Text> {

	@Override
    public int getPartition(VehicleIDTimestamp pair, 
                            Text text, 
                            int numberOfPartitions) {
    	// make sure that partitions are non-negative
        return Math.abs(pair.getVehicleID().hashCode() % numberOfPartitions);
    }

}