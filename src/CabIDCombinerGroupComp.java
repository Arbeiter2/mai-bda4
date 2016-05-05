import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CabIDCombinerGroupComp 
  	extends WritableComparator {

		 public CabIDCombinerGroupComp() {
		     super(CabIDTimestamp.class, true);
		 }
		

		 /**
		  * @param wc1 a WritableComparable object, which represnts a CabIDTimestamp
		  * @param wc2 a WritableComparable object, which represnts a CabIDTimestamp
		  * @return 0, 1, or -1 (depending on the comparsion of two CabIDTimestamp objects).
		  */
		 @Override
		 @SuppressWarnings("rawtypes")
		 public int compare(WritableComparable  wc1, WritableComparable wc2) {
			 CabIDTimestamp pair = (CabIDTimestamp) wc1;
			 CabIDTimestamp pair2 = (CabIDTimestamp) wc2;
			 
			 return (Integer.parseInt(pair.getVehicleID().toString()) - Integer.parseInt(pair2.getVehicleID().toString()));
		 }
  }