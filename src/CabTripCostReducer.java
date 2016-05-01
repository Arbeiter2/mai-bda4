import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CabTripCostReducer 
	extends Reducer<CabTripCostRecord, Text, Text, Text> {

	private Text keyStr = new Text();
	public void reduce(CabTripCostRecord key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text t : values)
		{
			keyStr.set(key.toString());
			context.write(keyStr, t);
		}
	}

}
