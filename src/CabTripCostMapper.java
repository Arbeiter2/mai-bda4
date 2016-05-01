import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CabTripCostMapper extends Mapper<Text, Text, Text, Text> {
	private Text trip_cost = new Text();
	private static String unit = "K";

	// K=km, N=nautical miles, M=statute mile
	private static double airport_lat;
	private static double airport_long;
	private static double airport_range;
	private static double taxi_start_charge;
	private static double taxi_charge_per_unit_dist;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		
		airport_lat = conf.getDouble("SFO_lat", 37.62131);
		airport_long = conf.getDouble("SFO_long", -122.37896);
		airport_range = conf.getDouble("SFO_range", 1d);
		taxi_start_charge = conf.getDouble("taxi_start_charge", 3.5);
		taxi_charge_per_unit_dist = conf.getDouble("taxi_charge_per_unit_dist", 1.71);
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
		long start_ts, end_ts;
		
		if (segStr == null)
			return null;
		
		String[] seg = segStr.split(";");
		retVal = new CabTripSegment[seg.length];
		
		// parse each of the six (6) comma-separated components
		for (String s : seg)
		{
			String[] bits = s.split(",");
			
			if (bits.length != 6)
				return null;
			
			start_ts = Long.parseLong(bits[0]);
			start_lat = Double.parseDouble(bits[1]);
			start_long = Double.parseDouble(bits[2]);			
			end_ts = Long.parseLong(bits[3]);
			end_lat = Double.parseDouble(bits[4]);
			end_long = Double.parseDouble(bits[5]);
			
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
	 * 		-1 if input malformed, or trip does not pass within range of airport
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
		
		boolean in_airport_range = false;
		
		// check each segment
		for (CabTripSegment s : segments)
		{
			start_ts = s.getStart_timestamp().get();
			start_lat = s.getStart_lat().get();
			start_long = s.getStart_long().get();
			
			end_ts = s.getEnd_timestamp().get();
			end_lat = s.getEnd_lat().get();
			end_long = s.getEnd_long().get();
			
			// reject dodgy timetamps
			if (start_ts >= end_ts)
				throw new IOException("Segment times invalid");
			
			// reject dodgy coordinates
			if (Math.abs(start_lat) >= 90d || Math.abs(start_long) > 180d ||
				Math.abs(end_lat) >= 90d   || Math.abs(end_long) > 180d)
				throw new IOException("Coordinates invalid");
			
			// it is possible for GPS samples to have gaps, so that end timestamp of one segment
			// does not match the start of next one; in this case, we must
			// calculate the distance between start of current sample and end of last one
			inter_seg_dist = 0d;
			if (last_ts != -999 && !(last_lat == start_lat && last_long == start_long))
			{
				inter_seg_dist = GeoDistanceCalc.distance(last_lat, last_lat, start_lat, start_long, unit);
				double tdiff = start_ts - last_ts;
				if (tdiff < 0)
					throw new IOException("Segment timestamps out of sequence");
				else if (tdiff > 600)
					throw new IOException("Gap between segments > 10 minutes: "+Double.toString(tdiff/60));
				
				// check whether this inter-segment journey passes within range of the airport
				if (GeoDistanceCalc.distanceFromLine(last_lat, last_long, start_lat, start_long, 
						airport_lat, airport_long, unit) <= airport_range)
					in_airport_range = true;
			}
			trip_length += inter_seg_dist;
			seg_dist = GeoDistanceCalc.distance(start_lat, start_long, end_lat, end_long, unit);
			
			// reject segments with average speed over 200 kmh
			if ((3600*seg_dist)/(end_ts-start_ts) > 200d)
				throw new IOException("Segment speed > 200 kph: "+Double.toString(seg_dist)+" km in "+Double.toString(end_ts-start_ts)+"s");
			trip_length += seg_dist;
			
			// check whether this segment journey passes within range of the airport
			if (GeoDistanceCalc.distanceFromLine(start_lat, start_long, end_lat, end_long, 
					airport_lat, airport_long, unit) <= airport_range)
				in_airport_range = true;			

			last_ts = end_ts;
			last_lat = end_lat;
			last_long = end_long;
		}
		
		// if the trip was not within tange of airport, return -1
		if (in_airport_range)
			return trip_length;
		else
			return -1d;
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
	 * 		<taxi-id>,<taxi-trip-number>,<start-timestamp>,<end-timestamp>,<distance>,<cost>
	 */	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// <taxi-id>, <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
		// . . . <end date> <end pos (lat)> <end pos (long)> <end status>

		// trip_ident should be <taxi_id>,<sequence-number>
		//System.out.println(key.toString()+"\t"+value.toString());

		if (key.toString().split(",").length < 2)
			throw new IOException("Malformed trip ident");
		
		// create segment objects from semicolon string list; bomb if any parse errors found
		CabTripSegment[] segments = parse(value.toString());
		if (segments == null)
			return;
		
		// calculate trip distance, and if valid, emit with trip ident and start time
		double dist;
		double cost;
		StringBuilder builder = new StringBuilder();
		try
		{
			dist = CabTripCostMapper.getTripLength(segments);
			if (dist == -1d)
				return;
			
			cost = taxi_start_charge + dist * taxi_charge_per_unit_dist;
			
			// construct record
			builder.setLength(0);
			builder.append(segments[0].getStart_timestamp().get());
			builder.append(",");
			builder.append(segments[0].getEnd_timestamp().get());
			builder.append(",");
			builder.append(dist);
			builder.append(",");
			builder.append(cost);
			
			trip_cost.set(builder.toString());
			
			context.write(key, trip_cost);
		} catch (IOException e)
		{
			// do nothing
			System.out.println(e.getMessage());
		}
	}
}
