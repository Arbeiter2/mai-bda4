import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CabTripCostRecordPartitioner 
	extends  Partitioner<CabTripCostRecord, Text> {
	@Override
    public int getPartition(CabTripCostRecord pair, 
                            Text data, 
                            int numberOfPartitions) {
    	// make sure that partitions are non-negative
		return (int)(pair.getStart_timestamp().get()) % numberOfPartitions;
    }

}
