import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Level;

public class CabTripRevenueReducer 
	extends Reducer<CabTripRevenueRecord, Text, Text, Text> {

	private Text keyStr = new Text();
	protected static DateFormat formatter = null;

	// output timestamps as epoch time; alternative is the following format:
	//
	// YYYY-MM-DDTHH:mm:ssZ
	// e.g. 2012-01-03T10:28+0300
	//
	protected static boolean epochTime = true;	
	
	@Override
	public void setup(Context context) {
	
		Configuration conf = context.getConfiguration();
		
		// by default use accumulated segment distance
		epochTime = conf.getBoolean("CabTripRevenue.epochTime", true);

		if (epochTime && formatter == null)
		{
			formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
		}
	}	
	
	public void reduce(CabTripRevenueRecord key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		for (Text t : values)
		{
			keyStr.set(key.toString(formatter));
			context.write(keyStr, t);
		}
	}

}
