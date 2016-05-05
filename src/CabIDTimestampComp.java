import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class CabIDTimestampComp 
	extends WritableComparator {

	 public CabIDTimestampComp() {
	     super(CabIDTimestamp.class, true);
	 }
	
	 @SuppressWarnings("rawtypes")
	 @Override
	 /**
	  * @param wc1 a WritableComparable object, which represnts a DateTemperaturePair
	  * @param wc2 a WritableComparable object, which represnts a DateTemperaturePair
	  * @return 0, 1, or -1 (depending on the comparsion of two DateTemperaturePair objects).
	  */
	 public int compare(WritableComparable  wc1, WritableComparable wc2) {
		 CabIDTimestamp pair = (CabIDTimestamp) wc1;
		 CabIDTimestamp pair2 = (CabIDTimestamp) wc2;
	     int diff = Integer.parseInt(pair.getVehicleID().toString()) - Integer.parseInt(pair2.getVehicleID().toString());
	     return diff;
	 }
}
