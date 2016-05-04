import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CabTripRevenueMapper extends Mapper<Text, Text, CabTripRevenueRecord, Text> {
	private static Logger theLogger = Logger.getLogger(CabTripRevenueMapper.class);

	private Text trip_cost = new Text();
	private static String trip_id;
	private static String unit = "K";
	private CabTripRevenueRecord timestamp_pair = new CabTripRevenueRecord();
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	private static Text tzStr = null;	// time zone string
	
	// true - use distance between start and end points of trip to calculate revenue
	// false - use accumulated segment distance to calculate trip length and revenue
	protected static boolean summaryOutput = false;
	
	// input timestamps as epoch time; alternative is the following format:
	//
	// YYYY-MM-DDTHH:mm:ssZ
	// e.g. 2012-01-03T10:28+0300
	//
	private static Boolean epochTime = null;

	// K=km, N=nautical miles, M=statute mile
	private static boolean useReference = false;
	private static String reference_name;
	private static double reference_lat;
	private static double reference_long;
	private static double reference_range;
	private static double taxi_start_charge;
	private static double taxi_charge_per_unit_dist;

	
	@Override
	public void setup(Context context) {
		theLogger.setLevel(Level.INFO);
		
		Configuration conf = context.getConfiguration();
		
		// if a reference point is supplied, only return costs within range
		useReference = conf.getBoolean("useReference", false);
		if (useReference)
		{
			theLogger.info("Using reference point ["+reference_name+"]");
			reference_name = conf.get("reference_name");
			reference_lat = conf.getDouble("reference_lat", 37.62131);
			reference_long = conf.getDouble("reference_long", -122.37896);
			reference_range = conf.getDouble("reference_range", 1d);
		}
		else
		{
			//System.out.println("No reference values passed");
		}
		taxi_start_charge = conf.getDouble("taxi_start_charge", 3.5);
		taxi_charge_per_unit_dist = conf.getDouble("taxi_charge_per_unit_dist", 1.71);
		
		
		// by default use accumulated segment distance
		summaryOutput = conf.getBoolean("CabTripRevenue.summaryOutput", false);
		
		/*
		StringBuilder s = new StringBuilder();
		s.append("useReference: ");
		s.append(useReference);
		s.append(", reference_name: ");
		s.append(reference_name);
		s.append(", reference_lat: ");
		s.append(reference_lat);
		s.append(", reference_long: ");
		s.append(reference_long);
		s.append(", reference_range: ");
		s.append(reference_range);
		s.append(", taxi_start_charge: ");
		s.append(taxi_start_charge);
		s.append(", taxi_charge_per_unit_dist: ");
		s.append(taxi_charge_per_unit_dist);
		
		System.out.println("mapper config: "+s.toString());
		*/
	}
	
	/**
	 * turn a semicolon separated list of segment data (without status codes) into 
	 * an array of CabTripSegments
	 * 
	 * @param segStr
	 * @return
	 */
	private static CabTripSegment[] parse(String segStr)
	{
		CabTripSegment[] retVal = null;
		ArrayList<CabTripSegment> segList = new ArrayList<CabTripSegment>();
		double start_lat, start_long, end_lat, end_long;
		long start_ts = 0;
		long end_ts = 0;
		
		if (segStr == null)
			return null;
		
		String[] seg = segStr.split(";");
		retVal = new CabTripSegment[seg.length];
		
		// parse each of the six (6) comma-separated components
		for (String s : seg)
		{
			String[] bits = s.split(" ");
			
			if (bits.length != 6)
				return null;
			
			// do coordinates first
			//start_ts = Long.parseLong(bits[0]);
			start_lat = Double.parseDouble(bits[1]);
			start_long = Double.parseDouble(bits[2]);			
			//end_ts = Long.parseLong(bits[3]);			
			end_lat = Double.parseDouble(bits[4]);
			end_long = Double.parseDouble(bits[5]);

			// reject dodgy coordinates
			if (Math.abs(start_lat) >= 90d || Math.abs(start_long) > 180d ||
				Math.abs(end_lat) >= 90d   || Math.abs(end_long) > 180d)
				return null;
			//throw new IOException("Coordinates invalid");
			
			// get timezone string
			if (tzStr == null)
			{
				tzStr = new Text();
				tzStr.set(TimezoneMapper.latLngToTimezoneString(start_lat, start_long));
				theLogger.info("CabTripRevenueMapper: using timezone ["+tzStr.toString()+"]");
			}

			// attempt to parse timestamps as numbers
			if (epochTime == null || epochTime)
			{
				try {
					start_ts = (long) Double.parseDouble(bits[0]);
					end_ts = (long) Double.parseDouble(bits[3]);
					if (epochTime == null)
						epochTime = new Boolean(true);
				} catch (NumberFormatException e)
				{
					if (epochTime)
					{
						theLogger.error( e.getMessage(), e );
						return null;
					}
					else						
						epochTime = new Boolean(false);
				}
			}
			
			
			// parse date strings
			if (!epochTime)
			{
				try {
					// parse dates and reject if they are invalid
					start_ts = formatter.parse(bits[0]).getTime()/1000;
					end_ts = formatter.parse(bits[3]).getTime()/1000;
				} catch (ParseException e) {
					theLogger.error( e.getMessage(), e );
					return null;
				}
			}
			
			// reject dodgy timetamps
			if (start_ts >= end_ts)
				return null;
			//throw new IOException("Segment times invalid");
			
						
			// we don't need to save the status codes as these are all in sorted time order
			segList.add(new CabTripSegment("", start_ts, start_lat, start_long,
					"",  end_ts, end_lat, end_long));
		}
		retVal = segList.toArray(new CabTripSegment[0]);
		
		return retVal;
	}
	
	
	
	/**
	 * calculates total distance travelled from comma-separated string of trip segments
	 * 
	 * @param segments - string representation of CabTripSegments
	 * @return - total distance, adding extra for non-following segments,
	 * 		-1 if input malformed, or trip does not pass within range of reference
	 * 
	 */
	protected static double getTripLength(CabTripSegment[] segments) throws IOException
	{
		double trip_length = 0d;
		double inter_seg_dist = 0d;
		double seg_dist = 0d;

		double start_ts, start_lat, start_long;
		double end_ts, end_lat, end_long;
		
		double last_lat = -999d;
		double last_long = -999d;
		double last_ts = -999d;
		
		boolean in_reference_range = false;
		
		if (segments == null || segments.length == 0)
			return -1d;
		
		
		// check each segment
		for (CabTripSegment s : segments)
		{
			start_ts = s.getStart_timestamp().get();
			start_lat = s.getStart_lat().get();
			start_long = s.getStart_long().get();
			
			end_ts = s.getEnd_timestamp().get();
			end_lat = s.getEnd_lat().get();
			end_long = s.getEnd_long().get();
			

			// it is possible for GPS samples to have gaps, so that end timestamp of one segment
			// does not match the start of next one; in this case, we must
			// calculate the distance between start of current sample and end of last one
			inter_seg_dist = 0d;
			if (last_ts != -999 && !(last_lat == start_lat && last_long == start_long))
			{
				inter_seg_dist = GeoDistanceCalc.distance(last_lat, last_long, start_lat, start_long, unit);
				double tdiff = start_ts - last_ts;
				if (tdiff < 0)
					throw new IOException("Segment timestamps out of sequence");
				else if (tdiff > 600)
					throw new IOException("Gap between segments > 10 minutes: "+Double.toString(tdiff/60));
				
				// check whether this inter-segment journey passes within range of the reference
				if (useReference && GeoDistanceCalc.distanceFromLine(last_lat, last_long, start_lat, start_long, 
						reference_lat, reference_long, unit) <= reference_range)
					in_reference_range = true;
			}
			trip_length += inter_seg_dist;
			seg_dist = GeoDistanceCalc.distance(start_lat, start_long, end_lat, end_long, unit);
			
			// reject segments with average speed over 200 kmh
			if ((3600*seg_dist)/(end_ts-start_ts) > 200d)
				throw new IOException("Segment speed > 200 kph: "+
						Double.toString(seg_dist)+" km in "+Double.toString(end_ts-start_ts)+"s");
			trip_length += seg_dist;
			
			// check whether this segment journey passes within range of the reference
			if (useReference && GeoDistanceCalc.distanceFromLine(start_lat, start_long, end_lat, end_long, 
					reference_lat, reference_long, unit) <= reference_range)
				in_reference_range = true;			

			last_ts = end_ts;
			last_lat = end_lat;
			last_long = end_long;
		}
		//System.out.println(trip_id.toString()+": i="+Double.toString(inter_seg_dist)+"; s="+Double.toString(seg_dist)+"; d="+Double.toString(trip_length));
	

		// if we are doing summary output, just use the start and end points of the segments
		if (summaryOutput)
		{
			start_ts = segments[0].getStart_timestamp().get();
			start_lat = segments[0].getStart_lat().get();
			start_long = segments[0].getStart_long().get();
			
			end_ts = segments[segments.length-1].getEnd_timestamp().get();
			end_lat = segments[segments.length-1].getEnd_lat().get();
			end_long = segments[segments.length-1].getEnd_long().get();
			
			trip_length = GeoDistanceCalc.distance(start_lat, start_long, end_lat, end_long, unit);
		}
	
		// if the trip was not within tange of reference, return -1
		if (useReference)
		{
			if (in_reference_range)
				return trip_length;
			else
				return -1d;
		}
		else
			return trip_length;
	}
	

	
	/*
	 * Output of CabTrips is in the following format
	 * 
	 * 		<taxi_id>,<sequence_number><\t><segment>[<segment>,...]
	 * 
	 * where <segment> is a tuple consisting of two components:
	 * 
	 * 		<segment> = <start-marker>,<end-marker>;
	 * 
	 * where <start-marker> and <end-marker> are both <marker> objects:
	 * 
	 * 		<marker> = <epoch time>,<latitude>,<longitude>
	 * 
	 * Thus complete segment consists of six (6) numerical components
	 * 
	 * 		<segment> = <start-epoch-time>,<start-latitude>,<start-longitude>,<end-epoch-time>,<end-latitude>,<end-longitude>
	 * 
	 * Output is in the following form:
	 * 
	 * 		<start-timestamp>,<end-timestamp>,<distance>,<cost>,<taxi-id>,<taxi-trip-number>
	 */	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// <taxi-id>, <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
		// . . . <end date> <end pos (lat)> <end pos (long)> <end status>

		if (key.toString().split(" ").length < 2)
			throw new IOException("Malformed trip ident");
		
		trip_id = key.toString();
		
		// create segment objects from semicolon string list; bomb if any parse errors found
		CabTripSegment[] segments = parse(value.toString());
		if (segments == null)
			return;
		
		// save time zone string
		timestamp_pair.setTimezoneStr(tzStr);
		
		// calculate trip distance, and if valid, emit with trip ident and start time
		double dist;
		double cost;
		StringBuilder builder = new StringBuilder();
		try
		{
			dist = CabTripRevenueMapper.getTripLength(segments);
			if (dist == -1d)
				return;
			
			cost = taxi_start_charge + dist * taxi_charge_per_unit_dist;
			
			// construct record
			builder.setLength(0);

			//builder.append(segments[segments.length-1].getEnd_timestamp().get());
			//builder.append(",");
			builder.append(dist);
			builder.append(" ");
			builder.append(cost);
			builder.append(" ");
			builder.append(trip_id);
			
			timestamp_pair.setStart_timestamp(segments[0].getStart_timestamp().get());
			timestamp_pair.setEnd_timestamp(segments[segments.length-1].getEnd_timestamp().get());
			
			trip_cost.set(builder.toString());
			
			context.write(timestamp_pair, trip_cost);
		} catch (IOException e)
		{
			// do nothing
			//theLogger.error( e.getMessage(), e );
		}
	}
}
