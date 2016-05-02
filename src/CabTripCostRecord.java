import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * @author Delano Greenidge
 *
 * Used for creating sorted reducer output for CabTripCost, order by start then end time stamps
 * 
 */
public class CabTripCostRecord 
	implements Writable, WritableComparable<CabTripCostRecord> {

    private LongWritable start_timestamp = new LongWritable();
	private LongWritable end_timestamp= new LongWritable();

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

	
	public CabTripCostRecord() {
	    }

    public CabTripCostRecord(long start_ts, long end_ts) {
        this.start_timestamp.set(start_ts);
        this.end_timestamp.set(end_ts);
    }

    public static CabTripCostRecord read(DataInput in) throws IOException {
        CabTripCostRecord pair = new CabTripCostRecord();
        pair.readFields(in);
        return pair;
    }

    //@Override
    public void write(DataOutput out) throws IOException {
    	start_timestamp.write(out);
    	end_timestamp.write(out);
        //tripData.write(out);
    }

    //@Override
    public void readFields(DataInput in) throws IOException {
    	start_timestamp.readFields(in);
        end_timestamp.readFields(in);
        //tripData.readFields(in);
    }

    //@Override
    public int compareTo(CabTripCostRecord pair) {
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

        CabTripCostRecord that = (CabTripCostRecord) o;
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
    	builder.append(",");
    	builder.append(end_timestamp);

    	return builder.toString();
    }    
}
