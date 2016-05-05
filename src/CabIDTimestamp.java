import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CabIDTimestamp 
    implements Writable, WritableComparable<CabIDTimestamp> {

    /**
     * tripData contains the following fields extracted from each record:
     * 
     * "start_lat" - start latitude
     * "start_long" - start longitude
     * "duration" - segment length in seconds
     * "start_status" - "E" (empty) or "M" (in use)
     * "end_status" - "E" (empty) or "M" (in use)
     * "end_lat" - end latitude
     * "end_long" - end longitude 
     * */
    //private Text tripData = new Text();
    private Text vehicleID = new Text(); 
    private LongWritable timestamp = new LongWritable();


    public CabIDTimestamp() {
    }

    public CabIDTimestamp(String vehicleID, long timestamp/*, Text m*/) {
        this.vehicleID.set(vehicleID);
        //this.tripData = m;
        this.timestamp.set(timestamp);
    }

    public static CabIDTimestamp read(DataInput in) throws IOException {
        CabIDTimestamp pair = new CabIDTimestamp();
        pair.readFields(in);
        return pair;
    }

    //@Override
    public void write(DataOutput out) throws IOException {
        vehicleID.write(out);
        timestamp.write(out);
        //tripData.write(out);
    }

    //@Override
    public void readFields(DataInput in) throws IOException {
        vehicleID.readFields(in);
        timestamp.readFields(in);
        //tripData.readFields(in);
    }

    //@Override
    public int compareTo(CabIDTimestamp pair) {
        int compareValue = Integer.parseInt(this.getVehicleID().toString()) -
			Integer.parseInt(pair.getVehicleID().toString());
        if (compareValue == 0) {
            compareValue = timestamp.compareTo(pair.gettimestamp());
        }
        return compareValue; 		// to sort ascending 
    }

    public Text getvehicleIDTimestamp() {
        return new Text(vehicleID.toString()+timestamp.toString());
    }
    
    public Text getVehicleID() {
        return vehicleID;
    }   
     
    /*
	public Text getTripData() {
        return tripData;
    }*/

    public LongWritable gettimestamp() {
        return timestamp;
    }

    public void setvehicleID(Text vehicleIDAsString) {
        vehicleID.set(vehicleIDAsString);
    }
    
    /*
	public void setTripData(Text m) {
    	tripData = m;
    }*/
    
    public void settimestamp(long timestamp) {
        this.timestamp.set(timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
           return true;
        }
        if (o == null || getClass() != o.getClass()) {
           return false;
        }

        CabIDTimestamp that = (CabIDTimestamp) o;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) {
           return false;
        }
        if (vehicleID != null ? !vehicleID.equals(that.vehicleID) : that.vehicleID != null) {
           return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = vehicleID != null ? vehicleID.hashCode() : 0;
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
    	StringBuilder builder = new StringBuilder();
    	builder.append("CabIDTimestamp{vehicleID=");
    	builder.append(vehicleID);
    	builder.append(", ts=");
    	builder.append(timestamp);
    	//builder.append(", text=[");
    	//builder.append(tripData);
    	builder.append("}");
    	return builder.toString();
    }
}
