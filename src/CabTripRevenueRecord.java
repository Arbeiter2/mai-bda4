import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * @author Delano Greenidge
 *
 * Used for creating sorted reducer output for CabTripCost, order by start then end time stamps
 * 
 */
public class CabTripRevenueRecord 
	implements Writable, WritableComparable<CabTripRevenueRecord> {

    private LongWritable start_timestamp = new LongWritable();
	private LongWritable end_timestamp= new LongWritable();
	private Text timezoneStr = new Text();
	private static TimeZone timeZone = null;

	public LongWritable getStart_timestamp() {
		return start_timestamp;
	}

	public void setStart_timestamp(long start_timestamp) {
		this.start_timestamp.set(start_timestamp);
	}

	public LongWritable getEnd_timestamp() {
		return end_timestamp;
	}

	public void setEnd_timestamp(long end_timestamp) {
		this.end_timestamp.set(end_timestamp);
	}


	public Text getTimezoneStr() {
		return timezoneStr;
	}

	public void setTimezoneStr(Text timezoneStr) {
		this.timezoneStr = timezoneStr;
	}    
	
	
	public CabTripRevenueRecord() {
	    }

    public CabTripRevenueRecord(long start_ts, long end_ts, String tzStr) {
        this.start_timestamp.set(start_ts);
        this.end_timestamp.set(end_ts);
        this.timezoneStr.set(tzStr);
    }

    public static CabTripRevenueRecord read(DataInput in) throws IOException {
        CabTripRevenueRecord pair = new CabTripRevenueRecord();
        pair.readFields(in);
        return pair;
    }

    //@Override
    public void write(DataOutput out) throws IOException {
    	start_timestamp.write(out);
    	end_timestamp.write(out);
        timezoneStr.write(out);
    }

    //@Override
    public void readFields(DataInput in) throws IOException {
    	start_timestamp.readFields(in);
        end_timestamp.readFields(in);
        timezoneStr.readFields(in);
    }

    //@Override
    public int compareTo(CabTripRevenueRecord pair) {
		int cmp = (int)(this.getStart_timestamp().get() - pair.getStart_timestamp().get());
		if (cmp == 0)
			cmp = (int)(this.getEnd_timestamp().get() - pair.getEnd_timestamp().get());
        return cmp; 		// to sort ascending 
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
           return true;
        }
        if (o == null || getClass() != o.getClass()) {
           return false;
        }

        CabTripRevenueRecord that = (CabTripRevenueRecord) o;
        if (start_timestamp != null ? !start_timestamp.equals(that.start_timestamp) : that.start_timestamp != null) {
           return false;
        }
        if (end_timestamp != null ? !end_timestamp.equals(that.end_timestamp) : that.end_timestamp != null) {
           return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = start_timestamp != null ? start_timestamp.hashCode() : 0;
        result = 37 * result + (end_timestamp != null ? end_timestamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
    	StringBuilder builder = new StringBuilder();
    	builder.append(start_timestamp);
    	builder.append(" ");
    	builder.append(end_timestamp);

    	return builder.toString();
    }
 
	/**
	 * @param epoch - seconds since 1970-01-01 00:00:00
	 * @param fmt - required format
	 * @return
	 */
	private String getFormattedDate(long epoch, DateFormat fmt)
	{
		if (fmt == null)
			return Long.toString(epoch);
		
		Date date = new Date(epoch * 1000L);
		timeZone = TimeZone.getTimeZone(timezoneStr.toString());
		fmt.setTimeZone(timeZone);
		return fmt.format(date);
	}
	
	/**
	 * creates string representation; parses timestamps into human readable form if given a DateFormat
	 * @param fmt
	 * @return
	 */
	public String toString(DateFormat fmt)
	{
		StringBuilder s = new StringBuilder();

		s.append(getFormattedDate(start_timestamp.get(), fmt));
		s.append(" ");
		s.append(getFormattedDate(end_timestamp.get(), fmt));
		
		
		return s.toString();
	}
}
