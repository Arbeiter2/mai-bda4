/**
 * @author Delano
 * 
 * records a single trip segment
 */
public class CabTripSegment {


	String start_status;	// "M" or "E"
	long start_timestamp;	// epoch time of segment start
	double start_lat;		// latitude at segment start
	double start_long;		// longitude at segment end

	String end_status;	// "M" or "E"
	long end_timestamp;	// epoch time of segment end
	double end_lat;		// latitude at segment end
	double end_long;		// longitude at segment end
	
	public CabTripSegment(String start_status, long start_timestamp, double start_lat, double start_long,
			String end_status, long end_timestamp, double end_lat, double end_long)
	{
		this.start_status = start_status;
		this.start_timestamp = start_timestamp;
		this.start_lat = start_lat;
		this.start_long = start_long;

		this.end_status = end_status;
		this.end_timestamp = end_timestamp;
		this.end_lat = end_lat;
		this.end_long = end_long;
	}
	
	@Override
	public String toString()
	{
		StringBuilder s = new StringBuilder();
		//s.append(start_status);
		//s.append(",");
		s.append(Long.toString(start_timestamp));
		s.append(",");
		s.append(Double.toString(start_lat));
		s.append(",");
		s.append(Double.toString(start_long));
		s.append(",");
		//s.append(end_status);
		//s.append(",");
		s.append(Long.toString(end_timestamp));
		s.append(",");
		s.append(Double.toString(end_lat));
		s.append(",");
		s.append(Double.toString(end_long));
		s.append(",");
		
		return s.toString();
	}
	
	public String getStart_status() {
		return start_status;
	}

	public long getStart_timestamp() {
		return start_timestamp;
	}

	public double getStart_lat() {
		return start_lat;
	}

	public double getStart_long() {
		return start_long;
	}

	public String getEnd_status() {
		return end_status;
	}

	public long getEnd_timestamp() {
		return end_timestamp;
	}

	public double getEnd_lat() {
		return end_lat;
	}

	public double getEnd_long() {
		return end_long;
	}	
	
}
