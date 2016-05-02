import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TimeZone;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author Delano
 *
 */
public class CabTripReducer
	extends Reducer<VehicleIDTimestamp, CabTripSegment, Text, Text> {

	private static Logger theLogger = Logger.getLogger(CabTripReducer.class);

	private Text taxi;
	private Text trip_id = new Text();
	private Text segmentString = new Text();
	
	// output date stamps with timezone in format [offset][hh:mm] 
	// e.g. 2010-12-23 09:12:09 -05:00 == EST
	protected static DateFormat formatter = null;
	
	/**
	 * records current trip ID for each taxi encountered in input
	 */
	protected HashMap<Text, Integer> tripCounter = new HashMap<Text, Integer>();
	protected HashMap<Text, Boolean> inTrip = new HashMap<Text, Boolean>();
	protected HashMap<Text, ArrayList<CabTripSegment>> segments = new HashMap<Text, ArrayList<CabTripSegment>>();

	
	@Override
	protected void setup(Context context)
            throws IOException,
            InterruptedException
    {
		theLogger.setLevel(Level.INFO);
	}
	
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
		//theLogger.info("S+Taxi["+taxi_id.toString()+"]::["+seg.toString() + "]");
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
		
		return true;
	}
	
	
	/**
	 * delete all segments from current trip
	 * @param taxi_id
	 */
	protected void clearSegments(Text taxi_id, boolean running)
	{
		ArrayList<CabTripSegment> segList = segments.get(taxi_id);
		if (segList != null)
		{
			//if (running)
			//	theLogger.info("clearSegments("+taxi_id.toString()+"): ["+Integer.toString(segList.size())+"]");

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
			return true;
		}

		// trash last trip if we start a new one in the middle of it
		Integer currTripNum = tripCounter.get(taxi_id);
		if (running)
		{
			//theLogger.info("startTrip("+taxi_id.toString()+"): closing nr ["+currTripNum.toString()+"]");
			clearSegments(taxi_id, running);
			return false;
		}
		else 
		{
			inTrip.put(taxi_id, true);
			if (currTripNum == null)
				currTripNum = new Integer(0);
			tripCounter.put(taxi_id, currTripNum+1);
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
		inTrip.put(taxi_id, false);
		clearSegments(taxi_id, false);
		return true;
	}
	
	/**
	 * emits output
	 * 
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void emit(Context context) throws IOException, InterruptedException
	{
		// build string out of array of segments
		CabTripSegment[] segList = getSegments(taxi);
		if (segList != null)
		{
			// create date parser if needed
			if (formatter == null)
			{
				formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
				
				// get timezone from lat/long
				String tz = TimezoneMapper.latLngToTimezoneString(segList[0].getStart_lat().get(), 
						segList[0].getStart_long().get());
				
				theLogger.info("CabTripReducer: Using timezone ["+tz+"]");

				// create timezone and assign to formatter
				TimeZone timeZone = TimeZone.getTimeZone(tz);
				formatter.setTimeZone(timeZone);
			}

			StringBuilder s = new StringBuilder();
			for (int i=0; i < segList.length-1; i++)
			{
				s.append(segList[i].toString(formatter));
				s.append(";");
			}
			
			// get output key as (taxi_id,taxi_trip_number)
			trip_id.set(getCurrentTripID(taxi));
			s.append(segList[segList.length-1]);
			segmentString.set(s.toString());

			//theLogger.info("R:emit("+trip_id.toString()+")["+Integer.toString(segList.length)+"]");
			
			// emit 
			context.write(trip_id, segmentString);
		}
		else
		{
			//System.out.println("Empty segList for "+taxi.toString());
		}
		
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
		//theLogger.info("R:"+key.toString() + "::" + values.toString());

		CabTripSegment last = null;
		boolean newTrip = true;
		for (CabTripSegment segment : values) {
			// <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
			// . . . <end date> <end pos (lat)> <end pos (long)> <end status>
			
			// discard segments starting before end of last one
			if (!newTrip && last != null 
					&& segment.getStart_timestamp().get() < last.getEnd_timestamp().get())
			{
				//theLogger.info("R:discard"+key.toString() + "[" + segment.toString()+"]");
				continue;
			}
			
			CabTripSegment seg = new CabTripSegment(segment);
		
			// check whether the end of the last segment was in the last hour
			String start_status = seg.getStart_status().toString();
			String end_status = seg.getEnd_status().toString();
			
			// meter started 
			if (start_status.equals("E") && end_status.equals("M"))
			{
				// newly started
				if (newTrip)
				{
					// then start a new one
					startTrip(taxi);
					addSegment(taxi, seg);
					newTrip = false;
				}
				else
				{
					// if meter start within record, and more than an hour has passed, new trip
					long gap = last.getEnd_timestamp().get() - seg.getStart_timestamp().get();
					if (gap >= 60L)
					{
						// output the trip we were last working on
						emit(context);
						
						// then start a new one
						startTrip(taxi);
						addSegment(taxi, seg);						
					}
					else
					{
						// continue existing trip
						addSegment(taxi, seg);
					}
				}
			}
			// meter running - on a trip
			else if (start_status.equals("M") && end_status.equals("M"))
			{
				addSegment(taxi, seg);
				newTrip = false;
			}
			// meter stopped during record - end of trip
			else if (start_status.equals("M") && end_status.equals("E"))
			{
				addSegment(taxi, seg);
				
				// emit current trip and close it
				emit(context);
				endTrip(taxi);
				newTrip = true;
			}
			last = seg;
		}
	}	
}
