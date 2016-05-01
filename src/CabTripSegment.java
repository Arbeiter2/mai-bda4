import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Delano
 * 
 * records a single trip segment
 */
public class CabTripSegment implements Writable {


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

    //@Override
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

    //@Override
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
		//s.append(start_status);
		//s.append(",");
		s.append(start_timestamp.toString());
		s.append(",");
		s.append(start_lat.toString());
		s.append(",");
		s.append(start_long.toString());
		s.append(",");
		//s.append(end_status);
		//s.append(",");
		s.append(end_timestamp.toString());
		s.append(",");
		s.append(end_lat.toString());
		s.append(",");
		s.append(end_long.toString());
		s.append(";");
		
		return s.toString();
	}
	
	public static boolean follows(CabTripSegment a, CabTripSegment b)
	{
		if (a == null || b == null)
			return false;

		return (a.end_lat.equals(b.start_lat) && a.end_long.equals(b.start_long));
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
