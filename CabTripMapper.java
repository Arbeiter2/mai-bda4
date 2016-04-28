import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CabTripMapper extends Mapper<Object, Text, VehicleIDTimestamp, Text> {

	private Text taxi_id = new Text();
	static private Text unused = new Text("");
	private LongWritable start_date = new LongWritable();
	private Text raw_data = new Text();
	private VehicleIDTimestamp vehicleTs = new VehicleIDTimestamp();
	protected static DateFormat formatter = new SimpleDateFormat("yyyy-MM-DD HH:mm:SS");

	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// <taxi-id>, <start date>, <start pos (lat)>, <start pos (long)>, <start status> . . .
		// . . . <end date> <end pos (lat)> <end pos (long)> <end status>
		String[] tokens = value.toString().replace("'", "").trim().split(",");
		//System.out.println(value.toString() + "\n" + key.toString());
		
		// discard records with no meter running
		//System.out.println(Integer.toString(tokens.length).toString() + "\t" + value.toString());
		if (tokens.length < 9)
			return;

		// keep only useful records
		if (!((tokens[4].equals("E") && tokens[8].equals("M")) ||
			  (tokens[4].equals("M") && tokens[8].equals("M")) ||
			  (tokens[4].equals("M") && tokens[8].equals("E"))))
			return;
		
		//System.out.println("[" + tokens[4] + "]\t[" + tokens[8] + "]");


		taxi_id.set(tokens[0]);
		
		// replace string representation of timestamps with string representation of epoch time
		long start = -1l;
		try {
			start = formatter.parse(tokens[1]).getTime()/1000;
			tokens[1] = Long.toString(start);
			tokens[5] = Long.toString(formatter.parse(tokens[5]).getTime()/1000);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Bad date string(s): "+ value.toString());
			return;
		}

		start_date.set(Long.parseLong(tokens[1]));
		
		// create a string consisting of all the fields comma separated in input  order
		raw_data.set(String.join(",", Arrays.copyOfRange(tokens, 1, tokens.length)));

		vehicleTs.setvehicleID(taxi_id);
		vehicleTs.settimestamp(start);
		//vehicleTs.setTripData(raw_data);
		
		//System.out.println(vehicleTs.toString());// + "\n" + raw_data.toString());
		context.write(vehicleTs, raw_data);
	}
}
