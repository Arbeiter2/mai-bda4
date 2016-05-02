import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CabTripCostReducer 
	extends Reducer<CabTripCostRecord, Text, Text, Text> {

	private Text keyStr = new Text();
	protected static DateFormat formatter = null;
	
	
	public void reduce(CabTripCostRecord key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if (formatter == null)
		{
			formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
		}
		
		for (Text t : values)
		{
			keyStr.set(key.toString(formatter));
			context.write(keyStr, t);
		}
	}

}
