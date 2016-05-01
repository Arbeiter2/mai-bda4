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
	extends Reducer<VehicleIDTimestamp, CabTripSegment, Text, Text> {


	private Text taxi;
	private Text trip_id = new Text();
	private Text segmentString = new Text();
	
	/**
	 * records current trip ID for each taxi encountered in input
	 */
	protected HashMap<Text, Integer> tripCounter = new HashMap<Text, Integer>();
	protected HashMap<Text, Boolean> inTrip = new HashMap<Text, Boolean>();
	protected HashMap<Text, ArrayList<CabTripSegment>> segments = new HashMap<Text, ArrayList<CabTripSegment>>();

	

	/**
	 * @param taxi_id
	 * @return String representation of trip ID
	 */
	protected String getCurrentTripID(Text taxi_id)
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
			return taxi_id.toString() + "," + currTripNum.toString();
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
		segList.add(seg);
	//System.out.println("S+Taxi["+taxi_id.toString()+"]::["+seg.toString() + "]("+Integer.toString(segList.size())+")");
		
		return true;
	}
	
	
	/**
	 * delete all segments from current trip
	 * @param taxi_id
	 */
	protected void clearSegments(Text taxi_id)
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
	protected CabTripSegment[] getSegments(Text taxi_id)
	{
		ArrayList<CabTripSegment> segList = segments.get(taxi_id);
		if (segList == null || segList.size() == 0)
		{
			return null;
		}

		CabTripSegment[] csArray = segList.toArray(new CabTripSegment[0]);
		return csArray;
	}
	
	
	/**
	 * add a new trip for this taxi; fail if in the middle of an incomplete trip
	 * 
	 * @param taxi_id
	 * @return true if trip start successful, false otherwise
	 */
	protected boolean startTrip(Text taxi_id)
	{
		Boolean running = inTrip.get(taxi_id);
		
		// brand new taxi on this reduce node
		if (running == null)
		{
			inTrip.put(taxi_id, true);
			tripCounter.put(taxi_id, 1);
			//System.out.println("++Taxi["+taxi_id.toString()+"]::Trip[1]");
			
			return true;
		}

		// trash last trip if we start a new one in the middle of it
		Integer currTripNum = tripCounter.get(taxi_id);
		if (running)
		{
			//inTrip.put(taxi_id, false);
			//System.out.println("!+Taxi["+taxi_id.toString()+"]::Trip[" + currTripNum.toString() + "]");
			clearSegments(taxi_id);
			
			return false;
		}
		else 
		{
			inTrip.put(taxi_id, true);
			currTripNum = currTripNum + 1;
			tripCounter.put(taxi_id, currTripNum);
			//System.out.println("++Taxi["+taxi_id.toString()+"]::Trip[" + currTripNum.toString() + "]");

			return true;
		}
	}


	/**
	 * mark a trip as complete; fail if not in a trip
	 * 
	 * @param taxi_id
	 * @return
	 */
	protected boolean endTrip(Text taxi_id)
	{
		//Integer currTripNum = tripCounter.get(taxi_id);
		//if (currTripNum != null)
		//	System.out.println("--Taxi["+taxi_id.toString()+"]::Trip[" + currTripNum.toString() + "]");
		//else
		//	System.out.println("Bad trip for taxi_id ["+taxi_id.toString()+"]");
		
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
	

	/**
	 * @param key VehicleIDTimetamp
	 * @param values all associated CabTripSegments for this VehicleID
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 * writes out complete trips in the following format
	 * 
	 * taxi_id,sequence_number<\t><segment>[,<segment>,...]
	 * 
	 * where <segment> is a tuple consisting of two components:
	 * 
	 * <segment> = <start-marker>,<end-marker>
	 * 
	 * where <start-marker> and <end-marker> are both <marker> objects:
	 * 
	 * <marker> = <epoch time>,<latitude>,<longitude>
	 * 
	 * Thus complete segment consists of six (6) numerical components
	 * 
	 * <segment> = <start-epoch-time>,<start-latitude>,<start-longitude>,<end-epoch-time>,<end-latitude>,<end-longitude>
	 */
	public void reduce(VehicleIDTimestamp key, Iterable<CabTripSegment> values, Context context)
			throws IOException, InterruptedException {

		taxi = key.getVehicleID();

		boolean retVal;
		for (CabTripSegment segment : values) {
			// <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
			// . . . <end date> <end pos (lat)> <end pos (long)> <end status>
			CabTripSegment seg = new CabTripSegment(segment);
			String start_status = seg.getStart_status().toString();
			String end_status = seg.getEnd_status().toString();
			
			// meter started within record - new trip
			if (start_status.equals("E") && end_status.equals("M"))
			{
				startTrip(taxi);
				addSegment(taxi, seg);
			}
			// meter running - on a trip
			else if (start_status.equals("M") && end_status.equals("M"))
			{
				addSegment(taxi, seg);
			}
			// meter stopped during record - end of trip
			else if (start_status.equals("M") && end_status.equals("E"))
			{
				//retVal = addSegment(taxi_id, new CabTripSegment(start, start_lat, start_long));
				retVal = addSegment(taxi, seg);
				
				// if we got a failure, delete current trip and return
				if (!retVal)
				{
					endTrip(taxi);
					return;
				}

				// build string out of array of segments
				CabTripSegment[] segList = getSegments(taxi);
				if (segList != null)
				{
					StringBuilder s = new StringBuilder();
					for (int i=0; i < segList.length-1; i++)
					{
						s.append(segList[i]);
						s.append(";");
					}
					System.out.println(s.toString());
					
					// get output key as (taxi_id,taxi_trip_number)
					trip_id.set(getCurrentTripID(taxi));
					s.append(segList[segList.length-1]);
					segmentString.set(s.toString());
					
					// emit 
					context.write(trip_id, segmentString);
				}
				else
				{
					System.out.println("Empty segList for "+taxi.toString());
				}
				
				endTrip(taxi);
			}
		}
	}
}
