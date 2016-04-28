import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CabTripDist {
	static String unit = "K"; // one of K=km, N=nautical miles, M=statute mile
	static int numBands = 100;
	static double minDist = 0.1;
	static double maxDist = 100d;
	static double sanityLimit = 200d;
	static double distBandLimits[] = null;
	

	protected static double[] getBandLimits() {
		double bands[] = new double[numBands];
		bands[0] = minDist;
		bands[numBands - 1] = maxDist;
		double bandWidth = (maxDist - minDist) / (numBands - 1);
		for (int i = 1; i < numBands - 1; i++) {
			bands[i] = minDist + bandWidth * i;
		}
		return bands;
	}

	protected static int getBand(double dist) {
		// bomb for trash values
		if (dist == 0d || dist > sanityLimit)
			return -1;

		// do ends first
		if (dist < distBandLimits[0])
			return 0;
		else if (dist >= distBandLimits[numBands - 1])
			return numBands - 1;

		// then middle
		for (int i = 1; i < numBands - 1; i++)
			if (dist < distBandLimits[i])
				return i;
		return -1;
	}

	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private IntWritable Band = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// # <taxi-id> <start date> <start pos (lat)> <start pos (long)> ...
			// # <end date> <end pos (lat)> <end pos (long)>
			String[] tokens = value.toString().trim().split(" ");
			double lat1 = Double.parseDouble(tokens[2]);
			double long1 = Double.parseDouble(tokens[3]);
			double lat2 = Double.parseDouble(tokens[5]);
			double long2 = Double.parseDouble(tokens[6]);

			System.out.println(value);
			double dist = GeoDistanceCalc.distance(lat1, long1, lat2, long2, unit);
			int bandNum = getBand(dist);
			if (bandNum != -1) {
				Band.set(bandNum);
				context.write(Band, one);
			}
		}
	}

	public static class CabIntSumReducer extends Reducer<IntWritable, IntWritable, DoubleWritable, IntWritable> {
		private DoubleWritable limit = new DoubleWritable();
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			limit.set(distBandLimits[key.get()]);
			context.write(limit, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length < 5)
		{
			System.out.println("Usage: CabTrips <input-file> <output-path> <number of bands> <min dist> <max dist>");
			return;
		}
		numBands = Integer.parseInt(args[2]);
		minDist = Double.parseDouble(args[3]);
		maxDist = Double.parseDouble(args[4]);

		// build limits if needed
		distBandLimits = getBandLimits();

		Job job = Job.getInstance(conf, "Cab trip length distribution");
		job.setJarByClass(CabTripDist.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(CabIntSumReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(CabIntSumReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileInputFormat.setMaxInputSplitSize(job, 100000);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
