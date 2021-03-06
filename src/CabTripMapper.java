import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.TimeZone;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class CabTripMapper 
	extends Mapper<Object, Text, CabIDTimestamp, CabTripSegment> {


	private static Logger theLogger = Logger.getLogger(CabTripMapper.class);
	private Text taxi_id = new Text();
	private CabIDTimestamp vehicleTs = new CabIDTimestamp();
	private ArrayList<Double> latitudeSamples = new ArrayList<Double>(); 
	private ArrayList<Double> longitudeSamples = new ArrayList<Double>();
	private long sampleCouter = 0;
	protected static DateFormat formatter = null; 
	private final static int SAMPLE_FREQUEBCY = 1000;
	

	@Override
	protected void setup(Context context)
            throws IOException,
            InterruptedException
    {
		theLogger.setLevel(Level.INFO);

		InputSplit is = context.getInputSplit();
		theLogger.info("M: splitId["+is.toString()+"]");
	}
	
	public void map(Object key, Text value, Context context) 
			throws IOException, 
			InterruptedException 
	{
		// <taxi-id>, <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
		// . . . <end date> <end pos (lat)> <end pos (long)> <end status>
		String[] tokens = value.toString().replace("'", "").trim().split(",");
		
		// discard records with too few fields
		if (tokens.length < 9)
			return;

		// keep only useful records
		if (!((tokens[4].equals("E") && tokens[8].equals("M")) ||
			  (tokens[4].equals("M") && tokens[8].equals("M")) ||
			  (tokens[4].equals("M") && tokens[8].equals("E"))))
			return;
		

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
			//e.printStackTrace();
			//System.out.println("Bad coordinates(s): "+ value.toString());
			return;
		}
		
		// reject segments with the same start/end time or same start/end GPS
		if ((start_lat == end_lat && start_long == end_long) || tokens[1].equals(tokens[5]))
			return;
		
		// create date parser if needed
		if (formatter == null)
		{
			formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			// get timezone from lat/long
			String tz = TimezoneMapper.latLngToTimezoneString(start_lat, start_long);
			
			// save timezone string
			Configuration conf = context.getConfiguration();
			conf.set("tz", tz);
			
			theLogger.info("CabTripMapper: Using timezone ["+tz+"] from coordinates ("+Double.toString(start_lat)+","+Double.toString(start_long)+")");

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
		
		if (sampleCouter++ % SAMPLE_FREQUEBCY == 0L)
		{
			latitudeSamples.add(start_lat);
			latitudeSamples.add(end_lat);
			longitudeSamples.add(start_long);
			longitudeSamples.add(end_long);
		}


		CabTripSegment seg = new CabTripSegment(tokens[4], start_epoch, start_lat, start_long, 
				tokens[8], end_epoch, end_lat, end_long);
		
		//System.out.println(taxi_id.toString()+","+seg.toString());
		vehicleTs.setvehicleID(taxi_id);
		vehicleTs.settimestamp(start_epoch);
		
		context.write(vehicleTs, seg);
	}

	
	@Override
	protected void cleanup(Context context)
			throws IOException, 
			InterruptedException 
	{
		super.cleanup(context);
		
		// calculate mean and variance of latitude and longitude
		Mean mean = new Mean();
		Variance var = new Variance();
		
		double lat[] = ArrayUtils.toPrimitive(latitudeSamples
				.toArray(new Double[latitudeSamples.size()]));
		
		double lng[] = ArrayUtils.toPrimitive(longitudeSamples
				.toArray(new Double[longitudeSamples.size()]));

		// write out file of statistics
		Configuration conf = context.getConfiguration();
		conf.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
		conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
		String geoDataFilePath = conf.get("geoDataFilePath", "hdfs:/tmp/cabtrips-geodata.csv");

        try{
            Path pt=new Path(geoDataFilePath);
            FileSystem fs = FileSystem.get(conf);
			org.apache.hadoop.fs.FSDataOutputStream stream;
			if ( fs.exists( pt )) 
				stream = fs.append(pt);
			else
				stream = fs.create(pt, true);
            BufferedWriter latFile=new BufferedWriter(new OutputStreamWriter(stream));
                                       // TO append data to a file, use fs.append(Path f)
            StringBuffer line = new StringBuffer();
            line.append(latitudeSamples.size());
            line.append(",");
            line.append(mean.evaluate(lat));
            line.append(",");
            line.append(mean.evaluate(lng));
            line.append(",");
            line.append(var.evaluate(lat));
            line.append(",");
            line.append(var.evaluate(lng));
            line.append("\n");
            
            latFile.write(line.toString());
            latFile.close();
	    }catch(Exception e){
	            System.out.println("Exception: "+e.toString());
	    }

        theLogger.info("geo.sample.size = "+ lat.length);

        theLogger.info("latitude.mean = "+ mean.evaluate(lat));
        theLogger.info("longitude.mean = "+ mean.evaluate(lng));

        theLogger.info("latitude.variance = "+ var.evaluate(lat));
        theLogger.info("longitude.variance = "+ var.evaluate(lng));
	}
}
