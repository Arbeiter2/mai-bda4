import java.io.IOException;
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
	 * calculates total distance travelled from comma-separated string of trip segments
	 * 
	 * @param segments - string representation of CabTripSegments
	 * @return - total distance, adding extra for non-following segments,
	 * 		-1 if input malformed, or trip does not pass within range of airport
	 * 
	 */
	protected static double getTripLength(String segments) throws IOException
	{
		String[] bits = segments.split(",");
		
		if (bits.length % 6 != 0)
			throw new IOException("Malformed segment text");
		
		double trip_length = 0d;
		double inter_seg_dist = 0d;
		double seg_dist = 0d;

		double start_ts;
		double start_lat;
		double start_long;
		double end_ts;
		double end_lat;
		double end_long;
		
		double last_lat = -999d;
		double last_long = -999d;
		double last_ts = -999d;
		
		boolean in_airport_range = false;
		
		for (int i=0; i < bits.length; i+=6)
		{
			start_ts = Double.parseDouble(bits[i]);
			start_lat = Double.parseDouble(bits[i+1]);
			start_long = Double.parseDouble(bits[i+2]);			
			end_ts = Double.parseDouble(bits[i+3]);
			end_lat = Double.parseDouble(bits[i+4]);
			end_long = Double.parseDouble(bits[i+5]);
			
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
	 * 		<taxi_id>,<sequence_number><\t><segment>[,<segment>,...]
	 * 
	 * where <segment> is a tuple consisting of two components:
	 * 
	 * 		<segment> = <start-marker>,<end-marker>
	 * 
	 * where <start-marker> and <end-marker> are both <marker> objects:
	 * 
	 * 		<marker> = <epoch time>,<latitude>,<longitude>
	 * 
	 * Thus complete segment consists of six (6) numerical components
	 * 
	 * 		<segment> = <start-epoch-time>,<start-latitude>,<start-longitude>,<end-epoch-time>,<end-latitude>,<end-longitude>
	 */	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// <taxi-id>, <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
		// . . . <end date> <end pos (lat)> <end pos (long)> <end status>

		// trip_ident should be <taxi_id>,<sequence-number>
		//System.out.println(key.toString()+"\t"+value.toString());

		if (key.toString().split(",").length < 2)
			throw new IOException("Malformed trip ident");
		
		// calculate trip distance, and if valid, emit with trip ident
		try
		{
		double dist = CabTripCostMapper.getTripLength(value.toString());
		double cost;
		if (dist!= -1d)
		{
			cost = taxi_start_charge + dist * taxi_charge_per_unit_dist;
			trip_cost.set(Double.toString(cost));
			
			context.write(key, trip_cost);
		}
		} catch (IOException e)
		{
			// do nothing
			System.out.println(e.getMessage());
		}
	}
}
