import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * @author Delano
 * 
 * records a single trip segment
 */
public class CabTripSegment implements Writable {

	private static Logger theLogger = Logger.getLogger(CabTripSegment.class);
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	private static Text tzStr = null;	// time zone string

    private Text start_status = new Text();  // "M" or "E"
    private LongWritable start_timestamp = new LongWritable();   // epoch time of segment start
    private DoubleWritable start_lat = new DoubleWritable();       // latitude at segment start
    private DoubleWritable start_long = new DoubleWritable();      // longitude at segment end

    private Text end_status = new Text();    // "M" or "E"
    private LongWritable end_timestamp = new LongWritable(); // epoch time of segment end
    private DoubleWritable end_lat = new DoubleWritable();     // latitude at segment end
    private DoubleWritable end_long = new DoubleWritable();        // longitude at segment end
    

	public CabTripSegment(String start_status, long start_timestamp, double start_lat, double start_long,
			String end_status, long end_timestamp, double end_lat, double end_long)
	{
        this.start_status.set(start_status);
        this.start_timestamp.set(start_timestamp);
        this.start_lat.set(start_lat);
        this.start_long.set(start_long);

        this.end_status.set(end_status);
        this.end_timestamp.set(end_timestamp);
        this.end_lat.set(end_lat);
        this.end_long.set(end_long);
	}

    public CabTripSegment(CabTripSegment seg) {
        this.start_status.set(seg.start_status.toString());
        this.start_timestamp.set(seg.start_timestamp.get());
        this.start_lat.set(seg.start_lat.get());
        this.start_long.set(seg.start_long.get());

        this.end_status.set(seg.end_status.toString());
        this.end_timestamp.set(seg.end_timestamp.get());
        this.end_lat.set(seg.end_lat.get());
        this.end_long.set(seg.end_long.get());
    }

    public CabTripSegment() {
    }

    public static CabTripSegment read(DataInput in) throws IOException {
        CabTripSegment seg = new CabTripSegment();
        seg.readFields(in);
        return seg;
    }


	/**
	 * turn a semicolon separated list of segment data (without status codes) into 
	 * an array of CabTripSegments
	 * 
	 * @param segStr
	 * @return
	 */
	public static CabTripSegment[] parse(String segStr)
	{
		// input timestamps as epoch time; alternative is the following format:
		//
		// YYYY-MM-DDTHH:mm:ssZ
		// e.g. 2012-01-03T10:28+0300
		//
		Boolean epochTime = null;
		
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
					if (epochTime != null)
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
			CabTripSegment sg= new CabTripSegment("", start_ts, start_lat, start_long,
			                    "",  end_ts, end_lat, end_long);
			segList.add(sg);
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
	public static double getTripLength(CabTripSegment[] segments, 
			boolean summaryOutput, boolean useReference,
			double reference_lat, double reference_long, 
			double reference_range, String unit) throws IOException
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
				if (useReference)
				{
					double ref_dist = GeoDistanceCalc.distanceFromLine(last_lat, last_long, start_lat, start_long, 
						reference_lat, reference_long, unit);
					if (Math.abs(ref_dist) <= reference_range)
					{
						in_reference_range = true;
						//theLogger.info("{ lat: "+Double.toString(last_lat)+", lng: "+Double.toString(last_long)+"}, \n"+
						//		"{ lat: "+Double.toString(start_lat)+", lng: "+Double.toString(start_long)+" },\n"+
						//		"ref_dist: "+Double.toString(ref_dist));
					}
				}
			}
			trip_length += inter_seg_dist;
			seg_dist = GeoDistanceCalc.distance(start_lat, start_long, end_lat, end_long, unit);
			
			// reject segments with average speed over 200 kmh
			if ((3600*seg_dist)/(end_ts-start_ts) > 200d)
				throw new IOException("Segment speed > 200 kph: "+
						Double.toString(seg_dist)+" km in "+Double.toString(end_ts-start_ts)+"s");
			trip_length += seg_dist;
			
			// check whether this segment journey passes within range of the reference
			if (useReference)
			{ 
				double ref_dist = GeoDistanceCalc.distanceFromLine(start_lat, start_long, end_lat, end_long, 
					reference_lat, reference_long, unit);
				if (ref_dist <= reference_range)
				{
					in_reference_range = true;
					//theLogger.info("{ lat: "+Double.toString(start_lat)+", lng: "+Double.toString(start_long)+"}, \n"+
					//		"{ lat: "+Double.toString(end_lat)+", lng: "+Double.toString(end_long)+" },\n"+
					//		"ref_dist: "+Double.toString(ref_dist));
				}
			}

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
	
		// if the trip was not within range of reference, return -1
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
    
    @Override
    public void write(DataOutput out) throws IOException {
        start_status.write(out);
        start_timestamp.write(out);
        start_lat.write(out);
        start_long.write(out);

        end_status.write(out);
        end_timestamp.write(out);
        end_lat.write(out);
        end_long.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        start_status.readFields(in);
        start_timestamp.readFields(in);
        start_lat.readFields(in);
        start_long.readFields(in);

        end_status.readFields(in);
        end_timestamp.readFields(in);
        end_lat.readFields(in);
        end_long.readFields(in);
    }
	
	@Override
	public String toString()
	{
		StringBuilder s = new StringBuilder();

		s.append(start_timestamp.toString());
		s.append(" ");
		s.append(start_lat.toString());
		s.append(" ");
		s.append(start_long.toString());
		s.append(" ");
		s.append(end_timestamp.toString());
		s.append(" ");
		s.append(end_lat.toString());
		s.append(" ");
		s.append(end_long.toString());
		
		return s.toString();
	}
	
	/**
	 * @param epoch - seconds since 1970-01-01 00:00:00
	 * @param fmt - required format
	 * @return
	 */
	public static String getFormattedDate(long epoch, DateFormat fmt)
	{
		Date date = new Date(epoch * 1000L);
		return fmt.format(date);
	}

	/**
	 * returns a record with formatted date string, instead of epoch time
	 * 
	 * @param fmt
	 * @return
	 */
	public String toString(DateFormat fmt)
	{
		if (fmt == null)
			return this.toString();

		StringBuilder s = new StringBuilder();

		s.append(getFormattedDate(start_timestamp.get(), fmt));
		s.append(" ");
		s.append(start_lat.toString());
		s.append(" ");
		s.append(start_long.toString());
		s.append(" ");
		s.append(getFormattedDate(end_timestamp.get(), fmt));
		s.append(" ");
		s.append(end_lat.toString());
		s.append(" ");
		s.append(end_long.toString());
		
		return s.toString();
	}
	

	public Text getStart_status() {
		return start_status;
	}

	public LongWritable getStart_timestamp() {
		return start_timestamp;
	}

	public DoubleWritable getStart_lat() {
		return start_lat;
	}

	public DoubleWritable getStart_long() {
		return start_long;
	}

	public Text getEnd_status() {
		return end_status;
	}

	public LongWritable getEnd_timestamp() {
		return end_timestamp;
	}

	public DoubleWritable getEnd_lat() {
		return end_lat;
	}

	public DoubleWritable getEnd_long() {
		return end_long;
	}	
	
}
