import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class CabTripDist {

	private static Logger theLogger = Logger.getLogger(CabTripDist.class);

	protected static double[] setBandLimits(int numBands, double maxDist, 
		double bandwidth)
	{
		double[] distBandLimits = new double[numBands];
		distBandLimits[0] = bandwidth;
		distBandLimits[numBands - 1] = maxDist;
		for (int i = 1; i < numBands - 1; i++) {
			distBandLimits[i] = bandwidth * (i+1);
		}
		return distBandLimits;
	}

	protected static int getBand(double dist, double sanityLimit, double[] bands) {
		// bomb for trash values
		if (dist == 0d || dist > sanityLimit)
			return -1;

		// do ends first
		if (dist < bands[0])
			return 0;
		else if (dist >= bands[bands.length - 1])
			return bands.length-1;

		// then middle
		for (int i = 1; i < bands.length-1; i++)
			if (dist < bands[i])
				return i;
		return -1;
	}

	public static class CabDistMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private IntWritable Band = new IntWritable();

		// K=km, N=nautical miles, M=statute mile
		private String unit;
		private int numBands;
		private double maxDist;
		private double bandwidth;
		private double sanityLimit;
		private double distBandLimits[];

		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			unit = conf.get("unit");
			maxDist = conf.getDouble("maxDist", 100d);
			bandwidth = conf.getDouble("bandwidth", 1d);
			numBands = (int)(maxDist/bandwidth) + 1;
			sanityLimit = conf.getDouble("sanityLimit", 200d);

			distBandLimits = setBandLimits(numBands, maxDist, bandwidth);

			// initialise counts to zero
			IntWritable zero = new IntWritable(0);
			try {
				for (int i=0; i < numBands; i++)
				{
					Band.set(i);
					context.write(Band, zero);
				}
			} catch (IOException e) {
				theLogger.error( e.getMessage(), e );

			} catch (InterruptedException e) {
				theLogger.error( e.getMessage(), e );
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			// # <taxi-id> <start date> <start pos (lat)> <start pos (long)> ...
			// # <end date> <end pos (lat)> <end pos (long)>
			String[] tokens = value.toString().trim().split(" ");
			double lat1 = Double.parseDouble(tokens[2]);
			double long1 = Double.parseDouble(tokens[3]);
			double lat2 = Double.parseDouble(tokens[5]);
			double long2 = Double.parseDouble(tokens[6]);

			double dist = GeoDistanceCalc.distance(lat1, long1, lat2,
				long2, unit);
			int bandNum = getBand(dist, sanityLimit, distBandLimits);
			if (bandNum != -1) {
				Band.set(bandNum);
				context.write(Band, one);
			}
		}
	}

	public static class CabDistReducer
			extends Reducer<IntWritable, IntWritable,
							Text, IntWritable> {
		private Text limit = new Text();
		private IntWritable result = new IntWritable();

		// K=km, N=nautical miles, M=statute mile
		private int numBands;
		private double maxDist;
		private double bandwidth;
		private double distBandLimits[];

		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			maxDist = conf.getDouble("maxDist", 100d);
			bandwidth = conf.getDouble("bandwidth", 1d);
			numBands = (int)(maxDist/bandwidth) + 1;

			distBandLimits = setBandLimits(numBands, maxDist, bandwidth);
		}

		public void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			int bandNumber = key.get();
			if (bandNumber == numBands - 1)
				limit.set("Infinity");
			else
				limit.set(Double.toString(distBandLimits[bandNumber]));
			context.write(limit, result);
		}
	}


	/*
	 * combines intermediate band/count pairs
	 *
	 */
	public static class CabDistCombiner
			extends Reducer<IntWritable, IntWritable,
							IntWritable, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		conf.setStrings("unit", "K");
		conf.setDouble("sanityLimit", 200d);
		double maxDist = 0d;
		double bandwidth = 0d;

		boolean goodArgs = false;
		if (args.length >= 4)
		{
			maxDist = Double.parseDouble(args[2]);
			bandwidth = Double.parseDouble(args[3]);
			if (maxDist > 0.0 && bandwidth > 0.0 && (int)(maxDist % bandwidth) == 0)
				goodArgs = true;
		}

		if (!goodArgs)
		{
			System.out.println("Usage: CabTripDist <input-file> <output-path> <max dist> <bandwidth>");
			System.out.println("<max-dist> must be a multiple of <bandwidth>");
			return;
		}

		conf.setDouble("maxDist", maxDist);
		conf.setDouble("bandwidth", bandwidth);

		Job job = Job.getInstance(conf, "Cab trip length distribution");

		// FileInputFormat.setMaxInputSplitSize(job, 100000);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(CabTripDist.class);
		job.setMapperClass(CabDistMapper.class);
		job.setCombinerClass(CabDistCombiner.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(CabDistReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
