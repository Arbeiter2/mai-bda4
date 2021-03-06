import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author Delano
 *
 */
public class CabTripReducer
	extends Reducer<CabIDTimestamp, CabTripSegment, Text, Text> {

	private static Logger theLogger = Logger.getLogger(CabTripReducer.class);

	private Text taxi;
	private Text trip_id = new Text();
	private Text segmentString = new Text();
	
	// output date stamps with timezone in format [offset][hh:mm] 
	// e.g. 2010-12-23 09:12:09 -05:00 == EST
	protected static DateFormat formatter = null;
	
	// returns output with only start and end GPS markers
	protected static boolean summaryOutput = true;
	
	// output timestamps as epoch time; alternative is the following format:
	// YYYY-MM-DDTHH:mm:ssZ
	// e.g. 2012-01-03T10:28+0300
	//
	protected static boolean epochTime = true;
	
	protected static long maxTripLength = -1;
	
	private final static double NUM_DEVIATIONS = 20d;
	
	// used for rejecting trips
	protected double minLatitude = -1;
	protected double maxLatitude = -1;
	protected double minLongitude = -1;
	protected double maxLongitude = -1;

	
	/**
	 * records current trip ID for each taxi encountered in input
	 */
	protected HashMap<Text, Integer> tripCounter = new HashMap<Text, Integer>();
	protected HashMap<Text, Boolean> inTrip = new HashMap<Text, Boolean>();
	protected HashMap<Text, ArrayList<CabTripSegment>> segments = new HashMap<Text, ArrayList<CabTripSegment>>();

	/**
	 * calculate pooled variance
	 * 
	 * @param variances
	 * @param sampleSizes
	 * @return
	 */
	private double getPooledVariance(ArrayList<Double> variances, ArrayList<Double> sampleSizes)
	{
		double numerator = 0d;
		double denominator = 0d;
		Iterator<Double> var = variances.iterator();
		Iterator<Double> n = sampleSizes.iterator();
		while (var.hasNext() && n.hasNext())
		{
			double nVal = n.next();
			numerator += var.next() * nVal;
			denominator += (nVal - 1);
		}
		return numerator/denominator;
	}
	
	/**
	 * @param means
	 * @return
	 */
	private double getPooledMean(ArrayList<Double> means)
	{
		double out = 0d;
		if (means.size() == 0)
			return 0d;
		
		for (Double mean : means)
		{
			out += mean;
		}		
		return out/means.size();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void setup(Context context)
            throws IOException,
            InterruptedException
    {
		theLogger.setLevel(Level.INFO);
		Configuration conf = context.getConfiguration();

		ArrayList<Double> sampleSizes = new ArrayList<Double>();
		ArrayList<Double> sampleLatMeans = new ArrayList<Double>();
		ArrayList<Double> sampleLatVariances = new ArrayList<Double>();
		ArrayList<Double> sampleLngMeans = new ArrayList<Double>();
		ArrayList<Double> sampleLngVariances = new ArrayList<Double>();

		// process geodata stats file 
		String geoDataFilePath = conf.get("geoDataFilePath", "hdfs:/tmp/cabtrips-geodata.csv");

		try {
			Path pt=new Path(geoDataFilePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line, fields[];
			line=br.readLine();
			while (line != null)
			{
				line = line.trim();
				fields = line.split(",");
				sampleSizes.add(Double.parseDouble(fields[0]));
				sampleLatMeans.add(Double.parseDouble(fields[1]));
				sampleLngMeans.add(Double.parseDouble(fields[2]));
				sampleLatVariances.add(Double.parseDouble(fields[3]));
				sampleLngVariances.add(Double.parseDouble(fields[4]));

				line=br.readLine();
			}
		}
		catch (Exception e)
		{
			System.out.println("Exception: "+e.toString());
		}

		// calculate pooled mean and variance
		double latitudeMean = getPooledMean(sampleLatMeans);
		double latitudeVariance = getPooledVariance(sampleLatVariances, sampleSizes);
		double longitudeMean = getPooledMean(sampleLngMeans);
		double longitudeVariance = getPooledVariance(sampleLngVariances, sampleSizes);
		
		if (sampleSizes.size() > 0)
		{
			minLatitude = latitudeMean - NUM_DEVIATIONS * Math.sqrt(latitudeVariance);
			maxLatitude = latitudeMean + NUM_DEVIATIONS * Math.sqrt(latitudeVariance);
			minLongitude = longitudeMean - NUM_DEVIATIONS * Math.sqrt(longitudeVariance);
			maxLongitude = longitudeMean + NUM_DEVIATIONS * Math.sqrt(longitudeVariance);
		}
		else
		{
			minLatitude = -90d;
			maxLatitude = 90d;
			minLongitude = -180d;
			maxLongitude = 180d;
		}
		
		theLogger.info("Lat range: ["+Double.toString(minLatitude)+", "+Double.toString(maxLatitude)+"]");
		theLogger.info("Long range: ["+Double.toString(minLongitude)+", "+Double.toString(maxLongitude)+"]");

		summaryOutput = conf.getBoolean("summaryOutput", true);
		epochTime = conf.getBoolean("epochTime", true);
		maxTripLength = conf.getLong("maxTripLength", -1);
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
			return taxi_id.toString() + " " + currTripNum.toString();
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

		// create new trip for this taxi if none currently active
		// it means we saw a sudden "M"-"M"
		if (running == null || !running)
		{
			startTrip(taxi_id);
		}
		
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
		if (segList == null)
			return;
		
		// reject trips exceeding maxTripLength
		if (maxTripLength > 0
			&& segList[segList.length-1].getEnd_timestamp().get() 
				- segList[0].getStart_timestamp().get() > maxTripLength)
			return;
	
		// create date parser if needed
		if (!epochTime && formatter == null)
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
		// generate giant ugly string
		// all segments are output, separated by semicolons
		if (!summaryOutput)
		{
			for (int i=0; i < segList.length-1; i++)
			{
				s.append(segList[i].toString(formatter));
				s.append(";");
			}
			s.append(segList[segList.length-1].toString(formatter));
		}
		else
		{
			// output only details of first and last waypoint
			// 9 1267402225.0 37.79076 -122.40255 1267402400.0 37.78538 -122.40024
			if (!epochTime)
				s.append(CabTripSegment.getFormattedDate(segList[0].getStart_timestamp().get(), formatter));
			else
				s.append(segList[0].getStart_timestamp());
			s.append(" ");
			s.append(segList[0].getStart_lat());
			s.append(" ");
			s.append(segList[0].getStart_long());
			s.append(" ");

			if (!epochTime)
				s.append(CabTripSegment.getFormattedDate(segList[segList.length-1].getStart_timestamp().get(), formatter));
			else
				s.append(segList[segList.length-1].getEnd_timestamp());
			s.append(" ");
			s.append(segList[segList.length-1].getEnd_lat());
			s.append(" ");
			s.append(segList[segList.length-1].getEnd_long());
		}
		
		// get output key as (taxi_id,taxi_trip_number)
		//trip_id.set(getCurrentTripID(taxi));
		trip_id.set(taxi);
		segmentString.set(s.toString());

		//theLogger.info("R:emit("+trip_id.toString()+")["+Integer.toString(segList.length)+"]");
		
		// emit 
		context.write(trip_id, segmentString);
		
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
	public void reduce(CabIDTimestamp key, Iterable<CabTripSegment> values, Context context)
			throws IOException, InterruptedException {

		taxi = key.getVehicleID();
		//theLogger.info("R:"+key.toString() + "::" + values.toString());

		CabTripSegment last = null;
		boolean newTrip = true;
		for (CabTripSegment segment : values) {
			// <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
			// . . . <end date> <end pos (lat)> <end pos (long)> <end status>
			
			// discard segments starting before end of last one; this happens when a "consolidated"
			// segment is followed by segments with the component subsegments 
			/* 
			 * consolidated segment A to C
			   9,'2010-03-15 15:06:56',37.61554,-122.38507,'E','2010-03-15 15:09:56',37.62008,-122.39949,'M'
			
			   component segments A to B, B to C - both of these are discarded
			   9,'2010-03-15 15:07:56',37.61708,-122.38764,'M','2010-03-15 15:08:56',37.61588,-122.38923,'M'
			   9,'2010-03-15 15:08:56',37.61588,-122.38923,'M','2010-03-15 15:09:56',37.62008,-122.39949,'M'
			   
			   segment C to D
			   9,'2010-03-15 15:09:56',37.62008,-122.39949,'M','2010-03-15 15:10:57',37.63574,-122.40345,'M'
			*/
			if (!newTrip && last != null 
					&& segment.getStart_timestamp().get() < last.getEnd_timestamp().get())
			{
				//theLogger.info("R:discard"+key.toString() + "[" + segment.toString()+"]");
				continue;
			}
			
			CabTripSegment seg = new CabTripSegment(segment);
			
			// reject all samples with coordinates very different from the majority
			if (seg.getStart_lat().get() < minLatitude || seg.getStart_lat().get() > maxLatitude 
			||  seg.getEnd_lat().get() < minLatitude || seg.getEnd_lat().get() > maxLatitude 
			||  seg.getStart_long().get() < minLongitude || seg.getStart_long().get() > maxLongitude 
			||  seg.getEnd_long().get() < minLongitude || seg.getEnd_long().get() > maxLongitude )
			{
				//theLogger.info("Rejecting "+seg);
				continue;
			}
			
			String start_status = seg.getStart_status().toString();
			String end_status = seg.getEnd_status().toString();
			
			// meter started 
			if (start_status.equals("E") && end_status.equals("M"))
			{
				// reject records with negative or super-long gap between start and end timestamps
				// 2811,'2010-02-27 23:58:57',37.75175,-122.39467,'E','2010-03-02 17:11:06',37.7832,-122.40298,'M'
				// 2811,'2010-03-02 17:11:06',37.7832,-122.40298,'M','2010-03-02 17:12:08',37.78255,-122.4019,'M' 
				long seg_gap = seg.getEnd_timestamp().get() - seg.getStart_timestamp().get();
				if (seg_gap < 0L || seg_gap > 600L)
					continue;

				// newly started
				if (newTrip)
				{
					// then start a new one
					startTrip(taxi);
					//addSegment(taxi, seg);
					newTrip = false;
					last = seg;
				}
				else
				{
					if (last != null)
					{
						// if meter starts within record, and more than 10 mins passed, new trip
						long gap = last.getEnd_timestamp().get() - seg.getStart_timestamp().get();
						if (gap >= 600L)
						{
							// output the trip we were last working on
							emit(context);
							
							// then start a new one
							startTrip(taxi);
							newTrip = true;
							last = null;
						}
					}
					else
					{
						last = seg;
					}
					//addSegment(taxi, seg);
				}
			}
			// meter running - on a trip
			else if (start_status.equals("M") && end_status.equals("M"))
			{
				/*
				meter stop/start may be missing, in which case gap between last segment end
				and start of the next will exceed ~5 minutes
				2008-06-09T13:38:37-0700 37.78856 -122.40897 2008-06-09T13:39:39-0700 37.78869 -122.40893
				2008-06-09T13:51:02-0700 37.77548 -122.42626 2008-06-09T13:51:58-0700 37.77509 -122.42952
				second sample represents a new trip
				*/
				if (last != null
					&& seg.getStart_timestamp().get() - last.getEnd_timestamp().get() >= 300L)
				{
					// output the trip we were last working on
					emit(context);

					// then start a new one
					startTrip(taxi);
					last = null;
					newTrip = true;
					addSegment(taxi, seg);
				}
				/*
				when gap between segment start and end > 1 hour, start new trip
				114,'2010-03-16 15:20:38',37.66164,-122.4024,'M','2010-03-16 15:21:38',37.67553,-122.38883,'M'
				114,'2010-03-16 15:21:38',37.67553,-122.38883,'M','2010-03-17 22:57:12',37.78033,-122.42369,'M'
				second segment is more than 24 hours long, so we start a new trip
				 */
				else if (seg.getEnd_timestamp().get() - seg.getStart_timestamp().get() > 3600L)
				{
					// output the trip we were last working on
					emit(context);

					// then start a new one
					startTrip(taxi);
					last = null;
					newTrip = true;
				}
				else
				{
					last = seg;
					newTrip = false;
					addSegment(taxi, seg);
				}
			}
			// meter stopped during record - end of trip
			else if ( (start_status.equals("M") && end_status.equals("E")) 
					// OR in the middle of a trip and suddenly meter is off
					||(!newTrip && start_status.equals("E") && end_status.equals("E")))
			{
				//addSegment(taxi, seg);
				
				// emit current trip and close it
				emit(context);
				endTrip(taxi);
				newTrip = true;
				last = null;
			}
		}
	}	
}
