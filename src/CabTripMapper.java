import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class CabTripMapper extends Mapper<Object, Text, VehicleIDTimestamp, CabTripSegment> {


	private static Logger theLogger = Logger.getLogger(CabTripMapper.class);
	private Text taxi_id = new Text();
	private VehicleIDTimestamp vehicleTs = new VehicleIDTimestamp();
	protected static DateFormat formatter = null; 
	

	@Override
	protected void setup(Context context)
            throws IOException,
            InterruptedException
    {
		theLogger.setLevel(Level.INFO);

		InputSplit is = context.getInputSplit();
		theLogger.info("M: splitId["+is.toString()+"]");
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// <taxi-id>, <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
		// . . . <end date> <end pos (lat)> <end pos (long)> <end status>
		String[] tokens = value.toString().replace("'", "").trim().split(",");
		//System.out.println(value.toString() + "\n" + key.toString());
		
		// discard records with no meter running
		//System.out.println(Integer.toString(tokens.length).toString() + "\t" + value.toString());
		if (tokens.length < 9)
			return;

		// keep only useful records
		if (!((tokens[4].equals("E") && tokens[8].equals("M")) ||
			  (tokens[4].equals("M") && tokens[8].equals("M")) ||
			  (tokens[4].equals("M") && tokens[8].equals("E"))))
			return;
		
		//System.out.println("[" + tokens[4] + "]\t[" + tokens[8] + "]");


		taxi_id.set(tokens[0]);
		
		
		// parse latitude/longitude values first; gives the opportunity to set timezone
		double start_lat = 0d;		// latitude at segment start
		double start_long = 0d;		// longitude at segment end
		double end_lat = 0d;		// latitude at segment end
		double end_long= 0d;		// longitude at segment end
		try {
			start_lat = Double.parseDouble(tokens[2]);
			start_long = Double.parseDouble(tokens[3]);
			end_lat = Double.parseDouble(tokens[6]);
			end_long = Double.parseDouble(tokens[7]);
			
			// fail if the lat/long is outside permitted range
			if (Math.abs(start_lat) > 90d || Math.abs(start_long) > 180d || 
				Math.abs(end_lat) > 90d || Math.abs(end_long) > 180d)
				return;
		} catch (NumberFormatException e) {
			e.printStackTrace();
			System.out.println("Bad coordinates(s): "+ value.toString());
			return;
		}
		
		// create date parser if needed
		if (formatter == null)
		{
			formatter = new SimpleDateFormat("yyyy-MM-DD HH:mm:SS");
			
			// get timezone from lat/long
			String tz = TimezoneMapper.latLngToTimezoneString(start_lat, start_long);

			// create timezone and assign to formatter
			TimeZone timeZone = TimeZone.getTimeZone(tz);
			formatter.setTimeZone(timeZone);
		}
		
		// parse string timestamps into epoch time, and latitude/longitude strings into longs
		long start_epoch = -1l;
		long end_epoch = -1l;
		try {
			// parse dates and reject if they are invalid
			start_epoch = formatter.parse(tokens[1]).getTime()/1000;
			end_epoch = formatter.parse(tokens[5]).getTime()/1000;
			if (!(end_epoch > start_epoch))
				return;
		} catch (ParseException e) {
			theLogger.error( e.getMessage(), e );
			return;
		}


		CabTripSegment seg = new CabTripSegment(tokens[4], start_epoch, start_lat, start_long, 
				tokens[8], end_epoch, end_lat, end_long);
		
		//System.out.println(taxi_id.toString()+","+seg.toString());
		vehicleTs.setvehicleID(taxi_id);
		vehicleTs.settimestamp(start_epoch);
		
		context.write(vehicleTs, seg);
	}
}
