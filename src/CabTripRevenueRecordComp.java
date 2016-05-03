import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class CabTripRevenueRecordComp 
	extends WritableComparator {

	 public CabTripRevenueRecordComp() {
	     super(CabTripRevenueRecord.class, true);
	 }
	
	 @SuppressWarnings("rawtypes")
	 @Override
	 /**
	  * @param wc1 a WritableComparable object, which represnts a CabTripRevenueRecord
	  * @param wc2 a WritableComparable object, which represnts a CabTripRevenueRecord
	  * @return 0, 1, or -1 (depending on the comparsion of two CabTripRevenueRecord objects).
	  */
	 public int compare(WritableComparable  wc1, WritableComparable wc2) {
		 CabTripRevenueRecord pair = (CabTripRevenueRecord) wc1;
		 CabTripRevenueRecord pair2 = (CabTripRevenueRecord) wc2;
		 
		 int cmp = (int)(pair.getStart_timestamp().get() - pair2.getStart_timestamp().get());
		 if (cmp == 0)
			 cmp = (int)(pair.getEnd_timestamp().get() - pair2.getEnd_timestamp().get());
		 
		 return cmp;
	 }
}