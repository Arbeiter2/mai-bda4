import java.io.IOException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class CabTripDist extends Configured implements Tool{
	
	private static Logger theLogger = Logger.getLogger(CabTripDist.class);
	private String inputPath = null;
	private String outputPath = null;
	private double maxDist = 0d;
	private double bandwidth = 0d;
	
	/**
	 * creates an array of frequency distribution upper limits.
	 * 
	 * @param numBands - number of band limits
	 * @param maxDist - upper limit
	 * @param bandwidth - width of bands
	 * @return
	 */	
	protected static double[] setBandLimits(int numBands, double maxDist, 
		double bandwidth)
	{
		double[] distBandLimits = new double[numBands];
		distBandLimits[0] = bandwidth;
		distBandLimits[numBands-1] = maxDist;
		for (int i = 1; i < numBands -1; i++) {
			distBandLimits[i] = bandwidth * (i+1);
		}
		return distBandLimits;
	}
	
	
	/**
	 * returns the index of the frequency band into which a value falls, using the provided bands
	 * For an output array a, using values v
	 *  
	 * 0 = v <= bandwidth
	 * 1 = bandwidth < v <= 2 * bandwidth
	 * 2 = 2 * bandwidth < v <= 3 * bandwidth
	 * ....
	 * numBands => maxDist-bandwidth < v <= maxDist
	 * numBands + 1 => v > MaxDist
	 *   
	 *  
	 * @param dist - value to be assigned to band
	 * @param sanityLimit - absolute upper limit; reject all above this
	 * @param bands - array of band limits
	 * @return band identifier, -1 if value rejected 
	 */
	protected static int getBand(double dist, double sanityLimit, double[] bands) {
		// bomb for trash values
		if (dist == 0d || dist > sanityLimit)
			return -1;

		// do ends first
		if (dist < bands[0])
			return 0;
		else if (dist > bands[bands.length - 1])
			return bands.length-1;

		// then middle
		for (int i = 1; i < bands.length-1; i++)
			if (dist < bands[i])
				return i;
		return -1;
	}
	
	
	/**
	 * CLI processing
	 * @return CLI Options
	 */
	private static Options buildOptions()
	{
		Options options = new Options();
		
		options.addOption("h", "help", false, "show help.");
		options.addOption("i", "input", true, "input path");
		options.addOption("o", "output", true, "output path");
		options.addOption("w", "width", true, "width of band e.g. 1.0");
		options.addOption("m", "maxdist", true, "lower bound of last band, i.e. 10 -> last band is for 10+;\nmust be integer multiple of width");

		return options;
	}
	
	/**
	 * print help message and exit
	 * @param options
	 */
	private static void help(Options options) 
	{
		// This prints out some help
		HelpFormatter formater = new HelpFormatter();
		
		formater.printHelp("CabTripDist", options);
		System.exit(0);
	}
	 
	/**
	 * process command line args, exit on failure
	 * @param args
	 */
	private void processArgs(String[] args) throws Exception {
		CommandLineParser parser = new BasicParser();
		Options options = buildOptions();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			theLogger.log(Level.INFO, "Failed to parse command line properties", e);
			System.out.println(e.getMessage());
			help(options);
		}
	
		if (cmd.hasOption("h"))
			help(options);

		if (cmd.hasOption("i")) {
			inputPath = cmd.getOptionValue("i");
		} else {
			theLogger.log(Level.INFO, "Missing -i option");
			help(options);
		}

		// output path
		if (cmd.hasOption("o")) {
			outputPath = cmd.getOptionValue("o");
		} else {
			theLogger.log(Level.INFO, "Missing -o option");
			help(options);
		}
		
		// verify paths
		if (inputPath.length() == 0 || outputPath.length() == 0 || inputPath.equals(outputPath))
		{
			theLogger.log(Level.INFO, "Invalid input/output path");
			help(options);				
		}
		
		// width
		if (cmd.hasOption("w")) {
			bandwidth = Double.parseDouble(cmd.getOptionValue("w"));
			if (bandwidth <= 0d)
			{
				theLogger.log(Level.INFO, "Invalid -w option");
				help(options);
			}
		}
		else
		{
			theLogger.log(Level.INFO, "Invalid -w option");
			help(options);
		}
		
		if (cmd.hasOption("m")) {
			maxDist = Double.parseDouble(cmd.getOptionValue("m"));
			if (maxDist <= 0d || (int)(maxDist % bandwidth) != 0)
			{
				theLogger.log(Level.INFO, "Invalid -m option");
				help(options);
			}
		}
		else
		{
			theLogger.log(Level.INFO, "Missing -w option");
			help(options);
		}
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

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			// # <taxi-id> <start date> <start pos (lat)> <start pos (long)> ...
			// # <end date> <end pos (lat)> <end pos (long)>
			String[] tokens = value.toString().trim().split(" ");
			double start_ts = Double.parseDouble(tokens[1]);
			double lat1 = Double.parseDouble(tokens[2]);
			double long1 = Double.parseDouble(tokens[3]);
			double end_ts = Double.parseDouble(tokens[4]);
			double lat2 = Double.parseDouble(tokens[5]);
			double long2 = Double.parseDouble(tokens[6]);

			double dist = GeoDistanceCalc.distance(lat1, long1, lat2,
				long2, unit);
			
			// ignore insanely short trips (less than 10m)
			if (dist < 0.01d)
				return;
			
			// check whether journey is plausible
			double duration = end_ts - start_ts;
			
			// ignore impossible timestamps
			if (duration <= 0d)
				return;
			
			// reject avg speed > 160 kmh
			double speed = 3600d * dist/duration;
			if (speed > 160d)
				return;
						
			// find relevant band identifier
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

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		conf.setStrings("unit", "K");
		conf.setDouble("sanityLimit", 200d);

		processArgs(args);

		conf.setDouble("maxDist", maxDist);
		conf.setDouble("bandwidth", bandwidth);

		Job job = Job.getInstance(conf, "Cab trip length distribution");

		// FileInputFormat.setMaxInputSplitSize(job, 100000);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setJarByClass(CabTripDist.class);
		job.setMapperClass(CabDistMapper.class);
		job.setCombinerClass(CabDistCombiner.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(CabDistReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean status = job.waitForCompletion(true);
		theLogger.info("run(): status="+status);
		return status ? 0 : 1;
	}
	
	
	
	/**
	* The main driver for word count map/reduce program.
	* Invoke this method to submit the map/reduce job.
	* @throws Exception When there is communication problems with the job tracker.
	*/
	public static int submitJob(String[] args) throws Exception {
		int returnStatus = ToolRunner.run(new CabTripDist(), args);
		return returnStatus;
	}
	
	
	public static void main(String[] args) throws Exception {
		// Make sure there are exactly 2 parameters
		int returnStatus = submitJob(args);
		theLogger.info("returnStatus="+returnStatus);
		
		System.exit(returnStatus);
	}	
	
}
