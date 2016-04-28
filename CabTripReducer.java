import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Delano
 *
 */
public class CabTripReducer 
	extends Reducer<VehicleIDTimestamp, Text, Text, Text> {


	private Text taxi_id;
	private Text segmentString = new Text();
	private Text values;
	
	/**
	 * @author Delano
	 * helper class for constructing list of segments
	 */
	protected class CabTripSegment {
		long timestamp;
		double latitude;
		double longitude;
		
		public CabTripSegment(long ts, double lat, double lng)
		{
			timestamp = ts;
			latitude = lat;
			longitude = lng;
		}
		
		@Override
		public String toString()
		{
			StringBuilder s = new StringBuilder();
			s.append(Long.toString(timestamp));
			s.append(",");
			s.append(Double.toString(latitude));
			s.append(",");
			s.append(Double.toString(longitude));
			
			return s.toString();
		}
	}
	
	/**
	 * records current trip ID for each taxi encountered in input
	 */
	protected static HashMap<Text, Integer> tripCounter = new HashMap<Text, Integer>();
	protected static HashMap<Text, Boolean> inTrip = new HashMap<Text, Boolean>();
	protected static HashMap<Text, ArrayList<CabTripSegment>> segments = new HashMap<Text, ArrayList<CabTripSegment>>();

	

	/**
	 * @param taxi_id
	 * @return String representation of trip ID
	 */
	protected static String getCurrentTripID(Text taxi_id)
	{
		Boolean running = inTrip.get(taxi_id);
		if (running == null || running.booleanValue() == false)
		{
			return null;
		}
		
		Integer currTripNum = tripCounter.get(taxi_id);
		if (currTripNum == null)
		{
			return null;
		}
		else
		{
			return taxi_id + "::" + currTripNum;
		}
	}


	/**
	 * add segment to current trip for taxi_id
	 * 
	 * @param taxi_id
	 * @param seg
	 */
	protected boolean addSegment(Text taxi_id, CabTripSegment seg)
	{
		Boolean running = inTrip.get(taxi_id);
		if (running == null || !running)
			return false;
		
		ArrayList<CabTripSegment> segList = segments.get(taxi_id);
		if (segList == null)
		{
			segList = new ArrayList<CabTripSegment>();
			segments.put(taxi_id, segList);
		}
		System.out.println("Adding segment ["+taxi_id.toString()+", "+seg.toString() + "]");
		segList.add(seg);
		
		return true;
	}
	
	
	/**
	 * delete all segments from current trip
	 * @param taxi_id
	 */
	protected static void clearSegments(Text taxi_id)
	{
		ArrayList<CabTripSegment> segList = segments.get(taxi_id);
		if (segList != null)
		{
			segList.clear();
		}
	}
	
	/**
	 * return current trip segments as an array
	 * 
	 * @param taxi_id
	 * @return array of CabTripSegments, or null if no segments found
	 */
	protected static CabTripSegment[] getSegments(Text taxi_id)
	{
		ArrayList<CabTripSegment> segList = segments.get(taxi_id);
		if (segList != null && segList.size() > 0)
		{
			CabTripSegment[] csArray = segList.toArray(new CabTripSegment[0]);
			return csArray;
		}
		else
		{
			return null;
		}
	}
	
	
	/**
	 * add a new trip for this taxi; fail if in the middle of an incomplete trip
	 * 
	 * @param taxi_id
	 * @return true if trip start successful, false otherwise
	 */
	protected static boolean startTrip(Text taxi_id)
	{
		Boolean running = inTrip.get(taxi_id);
		
		// brand new taxi on this reduce node
		if (running == null)
		{
			inTrip.put(taxi_id, true);
			tripCounter.put(taxi_id, 1);
			System.out.println("Start trip 1 for taxi_id ["+taxi_id.toString()+"]");
			
			return true;
		}

		// trash last trip if we start a new one in the middle of it
		if (running)
		{
			inTrip.put(taxi_id, false);
			clearSegments(taxi_id);
			
			return false;
		}
		else 
		{
			Integer currTripNum = tripCounter.get(taxi_id) + 1;
			inTrip.put(taxi_id, true);
			tripCounter.put(taxi_id, currTripNum);
			System.out.println("Start trip " + currTripNum.toString() + " for taxi_id ["+taxi_id.toString()+"]");

			return true;
		}
	}


	/**
	 * mark a trip as complete; fail if not in a trip
	 * 
	 * @param taxi_id
	 * @return
	 */
	protected static boolean endTrip(Text taxi_id)
	{
		Integer currTripNum = tripCounter.get(taxi_id);
		if (currTripNum == null)
			System.out.println("Bad trip for taxi_id ["+taxi_id.toString()+"]");
		else
			System.out.println("Ending trip " + currTripNum.toString() + " for taxi_id ["+taxi_id.toString()+"]");
		// brand new taxi on this reduce node
		//if (running != null && running)
		//{
			inTrip.put(taxi_id, false);
			clearSegments(taxi_id);
			return true;
		//}
		//else
		//{
			//return false;
		//}
		
		
	}	
	

	public void reduce(VehicleIDTimestamp key, Iterable<Text> v, Context context)
			throws IOException, InterruptedException {

		taxi_id = key.getVehicleID();
		//values = key.getTripData();
		boolean retVal;
		/*for (Text val : v) {
			System.out.println(taxi_id.toString() + "\t" + val);
		}*/
		
		for (Text values : v) {
			String[] tokens = values.toString().replace("'", "").trim().split(",");
			// <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
			// . . . <end date> <end pos (lat)> <end pos (long)> <end status>
			long start = Long.parseLong(tokens[0]);
			double start_lat = Double.parseDouble(tokens[1]); 
			double start_long = Double.parseDouble(tokens[2]);
			String start_status = tokens[3];
			
			long end = Long.parseLong(tokens[4]);
			double end_lat = Double.parseDouble(tokens[5]);			
			double end_long = Double.parseDouble(tokens[6]);
			String end_status = tokens[7];
			
			if (start_status.equals("E") && end_status.equals("M"))
			{
				startTrip(taxi_id);
				addSegment(taxi_id, new CabTripSegment(end, end_lat, end_long));
			}
			else if (start_status.equals("M") && end_status.equals("M"))
			{
				addSegment(taxi_id, new CabTripSegment(end, end_lat, end_long));
			}
			else if (start_status.equals("M") && end_status.equals("E"))
			{
				retVal = addSegment(taxi_id, new CabTripSegment(start, start_lat, start_long));
				
				// if we got a failure, delete current trip and return
				if (!retVal)
				{
					endTrip(taxi_id);
					return;
				}

				// build string out of array of segments
				CabTripSegment[] segList = getSegments(taxi_id);
				if (segList != null)
				{
					StringBuilder s = new StringBuilder();
					for (int i=0; i < segList.length-1; i++)
					{
						s.append(segList[i]);
						s.append(",");
					}
					s.append(segList[segList.length-1]);
					segmentString.set(s.toString());
					
					// emit 
					context.write(taxi_id, segmentString);
				}
				else
				{
					System.out.println("Empty segList for "+taxi_id.toString());
				}
				
				endTrip(taxi_id);
			}
		}
	}
}
