import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * 
 */

/**
 * @author Delano Greenidge
 *
 */
public class CabTripRevenue extends Configured implements Tool {
	
	private static Logger theLogger = Logger.getLogger(CabTripRevenue.class);
	private String inputPath = null;
	private String outputPath = null;
	private int numReducers = 1;
	private double initialCharge = 0.00;
	private double chargePerKm = 0.00;
	
	// by default cost calculator finds cost of all trips; 
	// if reference point is provided, only trips passing within provided range around
	// provided lat/long reference point will be returned
	private String referenceName = null;
	private double referenceLat = -9999d;
	private double referenceLong = -9999d;
	private double referenceRangeKm = -1d;	
	
	
	
	private static Options buildOptions()
	{
		Options options = new Options();
		
		options.addOption("h", "help", false, "show help.");
		options.addOption("i", "input", true, "input path");
		options.addOption("o", "output", true, "output path");
		options.addOption("r", "reducers", true, "number of reducers");
		options.addOption("C", "charge", true, "taxi charge (format: <initial charge>,<cost-per-km>)");
		options.addOption("L", "location", true, "reference location, and range from ref (format: <string-ref>,<lat>,<long>,<range-km>)");

		return options;
	}
		
	private static void help(Options options) 
	{
		// This prints out some help
		HelpFormatter formater = new HelpFormatter();
		
		formater.printHelp("CabTripCost", options);
		System.exit(0);
	}
	 
	/**
	 * @param args
	 */
	private void processArgs(String[] args) throws Exception {
		CommandLineParser parser = new BasicParser();
		Options options = buildOptions();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);

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
			
			// numReducers
			if (cmd.hasOption("r")) {
				numReducers = Integer.parseInt(cmd.getOptionValue("r"));
				if (numReducers <= 0)
				{
					theLogger.log(Level.INFO, "Invalid -r option");
					help(options);
				}
			}

			// charges
			if (cmd.hasOption("C")) {
				String[] bits = cmd.getOptionValue("C").split(",");
				if (bits.length < 2)
				{
					theLogger.log(Level.INFO, "Invalid -C option");
					help(options);
				}
				
				initialCharge = Double.parseDouble(bits[0]);
				chargePerKm = Double.parseDouble(bits[1]);
				if (initialCharge < 0d || chargePerKm <= 0d)
				{
					theLogger.log(Level.INFO, "Bad trip charge values");
					help(options);
				}
			}
			else
			{
				help(options);
			}
			
			// location
			if (cmd.hasOption("L")) {
				String[] bits = cmd.getOptionValue("L").split(",");
				if (bits.length < 4)
				{
					theLogger.log(Level.INFO, "Invalid -L option");
					help(options);
				}
				
				referenceName = bits[0];
				referenceLat = Double.parseDouble(bits[1]);
				referenceLong = Double.parseDouble(bits[2]);
				referenceRangeKm = Double.parseDouble(bits[3]);
				
				if (Math.abs(referenceLat) >= 90d 
				||  Math.abs(referenceLong) > 180d 
				||  Math.abs(referenceLong) <= 0d)
				{
					theLogger.log(Level.INFO, "Invalid lat/long or range");
					help(options);
				}
			}
		} catch (ParseException e) {
			theLogger.log(Level.INFO, "Failed to parse command line properties", e);
			help(options);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        conf.set("mapreduce.output.key.field.separator", ",");
        conf.set("mapreduce.textoutputformat.separator", ","); 
        conf.set("mapred.textoutputformat.separator", ",");
        
        // process cmdline
        processArgs(args);

		// used for calculating taxi cost
		if (referenceName != null)
		{
			System.out.println("Using reference location ["+referenceName+"]");
			
			conf.setBoolean("useReference", true);
			conf.set("reference_name", referenceName);
			conf.setDouble("reference_lat", referenceLat);
			conf.setDouble("reference_long", referenceLong);
			conf.setDouble("reference_range", referenceRangeKm);
			conf.setDouble("taxi_start_charge", initialCharge);
			conf.setDouble("taxi_charge_per_unit_dist", chargePerKm);
		}
		
		// create Job *after* configuration is complete
		Job job = Job.getInstance(conf, "Cab trip cost calc");
	    FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		// use user-supplied number of reduce tasks
		if (numReducers > 1)
		{
			job.setNumReduceTasks(numReducers);
		}

		job.setJarByClass(CabTripRevenue.class);
		job.setJobName("CabTripCost ["+inputPath+"], R"+Integer.toString(numReducers));

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		
		//job.setMapOutputKeyClass(CabTripRevenueRecord.class);
		//job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(CabTripRevenueRecord.class);
		job.setOutputValueClass(Text.class);

        job.setMapperClass(CabTripRevenueMapper.class);
        job.setReducerClass(CabTripRevenueReducer.class); 
    	job.setPartitionerClass(CabTripRevenueRecordPartitioner.class);
    	job.setGroupingComparatorClass(CabTripRevenueRecordComp.class);

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
		int returnStatus = ToolRunner.run(new CabTripRevenue(), args);
		return returnStatus;
	}	
		
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		int returnStatus = submitJob(args);
		theLogger.info("returnStatus="+returnStatus);
		
		System.exit(returnStatus);
	}

}
