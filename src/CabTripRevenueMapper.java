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
			reference_name = conf.get("reference_name");
			reference_lat = conf.getDouble("reference_lat", 37.62131);
			reference_long = conf.getDouble("reference_long", -122.37896);
			reference_range = conf.getDouble("reference_range", 1d);
			theLogger.info("Using reference point ["+reference_name+"]");
		}
		else
		{
			//System.out.println("No reference values passed");
		}
		taxi_start_charge = conf.getDouble("taxi_start_charge", 3.5);
		taxi_charge_per_unit_dist = conf.getDouble("taxi_charge_per_unit_dist", 1.71);
		
		
		// by default use accumulated segment distance
		summaryOutput = conf.getBoolean("CabTripRevenue.summaryOutput", false);
	}
	
	public static boolean isSummaryOutput() {
		return summaryOutput;
	}

	public static void setSummaryOutput(boolean summaryOutput) {
		CabTripRevenueMapper.summaryOutput = summaryOutput;
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

		//if (key.toString().split(" ").length < 2)
		//	throw new IOException("Malformed trip ident");
		
		// now just a taxi id
		trip_id = key.toString();
		
		// create segment objects from semicolon string list; bomb if any parse errors found
		CabTripSegment[] segments = CabTripSegment.parse(value.toString());
		if (segments == null)
			return;
		
		// save time zone string
		if (tzStr == null)
		{
			tzStr = new Text();
			tzStr.set(TimezoneMapper.latLngToTimezoneString(segments[0].getStart_lat().get(), segments[0].getStart_long().get()));
			theLogger.info("CabTripRevenueMapper: using timezone ["+tzStr.toString()+"]");
		}		
		timestamp_pair.setTimezoneStr(tzStr);
		
		// calculate trip distance, and if valid, emit with trip ident and start time
		double dist;
		double cost;
		StringBuilder builder = new StringBuilder();
		try
		{
			dist = CabTripSegment.getTripLength(segments, summaryOutput, useReference, reference_lat, reference_long, reference_range, "K");
			if (dist == -1d)
				return;
			
			cost = taxi_start_charge + dist * taxi_charge_per_unit_dist;
			
			// construct record
			builder.setLength(0);

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
