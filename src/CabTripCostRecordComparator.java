import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class CabTripCostRecordComparator 
	extends WritableComparator {

	 public CabTripCostRecordComparator() {
	     super(CabTripCostRecord.class, true);
	 }
	
	 @SuppressWarnings("rawtypes")
	 @Override
	 /**
	  * @param wc1 a WritableComparable object, which represnts a CabTripCostRecord
	  * @param wc2 a WritableComparable object, which represnts a CabTripCostRecord
	  * @return 0, 1, or -1 (depending on the comparsion of two CabTripCostRecord objects).
	  */
	 public int compare(WritableComparable  wc1, WritableComparable wc2) {
		 CabTripCostRecord pair = (CabTripCostRecord) wc1;
		 CabTripCostRecord pair2 = (CabTripCostRecord) wc2;
		 
		 int cmp = pair.getStart_timestamp().compareTo(pair2.getStart_timestamp());
		 if (cmp == 0)
			 cmp = pair.getEnd_timestamp().compareTo(pair2.getEnd_timestamp());
		 
		 return cmp;
	 }
}