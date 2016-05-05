import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Delano Greenidge
 *
 * This class is dodgy as hell. It gets the output from the mapper, and builds a linked list from the 
 * CabTripSegmens it receives.
 */
public class CabIDCombiner 
	extends Reducer<CabIDTimestamp, CabTripSegment, CabIDTimestamp, CabTripSegment> {
	
	private CabTripSegment result = null;
	
	@Override
	public void reduce(CabIDTimestamp key, Iterable<CabTripSegment> values,
	Context context) throws IOException, InterruptedException {
		
		CabTripSegment last = null;
		String taxi = key.getVehicleID().toString();
		for (CabTripSegment val : values) 
		{
			
			if (result == null)
			{
				result = last = val;
				
				continue;
			}
			last.append(val);
			last = val;
			
			context.write(key, val);
		}
		//context.write(key, result);
		
		while (result != null)
		{
			System.out.println("CabIDCombiner: "+taxi+": "+result.getStart_timestamp().toString());
		}
	}	
}
