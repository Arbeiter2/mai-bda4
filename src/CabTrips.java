import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * @author Delano Greenidge
 *
 */
public class CabTrips extends Configured implements Tool{
	
	private static Logger theLogger = Logger.getLogger(CabTrips.class);
	private String inputPath = null;
	private String outputPath = null;
	private int numReducers = 1;
	
	private static Options buildOptions()
	{
		Options options = new Options();
		
		options.addOption("h", "help", false, "show help.");
		options.addOption("i", "input", true, "input path");
		options.addOption("o", "output", true, "output path");
		options.addOption("r", "reducers", true, "number of reducers");

		return options;
	}
		
	private static void help(Options options) 
	{
		// This prints out some help
		HelpFormatter formater = new HelpFormatter();
		
		formater.printHelp("CabTrips", options);
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
		} catch (ParseException e) {
			theLogger.log(Level.INFO, "Failed to parse command line properties", e);
			help(options);
		}
	}
	
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
		processArgs(args);
		
		Job job = Job.getInstance(conf, "Cab trip builder");
	    FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// use user-supplied number of reduce tasks
		if (numReducers > 1)
		{
			job.setNumReduceTasks(numReducers);
		}
		
		job.setJarByClass(CabTrips.class);
		job.setJobName("CabTrips ["+inputPath+"], R"+Integer.toString(numReducers));

		job.setOutputKeyClass(VehicleIDTimestamp.class);
		job.setOutputValueClass(CabTripSegment.class);
		
    	job.setMapperClass(CabTripMapper.class);
    	job.setReducerClass(CabTripReducer.class);		
    	job.setPartitionerClass(VehicleIDTimestampPartitioner.class);
    	job.setGroupingComparatorClass(VehicleIDTimestampComp.class);

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
		int returnStatus = ToolRunner.run(new CabTrips(), args);
		return returnStatus;
	}	
	
	/**
	* The main driver for word count map/reduce program.
	* Invoke this method to submit the map/reduce job.
	* @throws Exception When there is communication problems with the job tracker.
	*/
	public static void main(String[] args) throws Exception {
		// Make sure there are exactly 2 parameters
		if (args.length < 2) {
			theLogger.warn("CabTrips <input-file> <output-dir> [<num-reduce-tasks>]");
			throw new IllegalArgumentException("Usage: CabTrips <input-dir> <output-dir> [<num-reduce-tasks>]");
		}

		int returnStatus = submitJob(args);
		theLogger.info("returnStatus="+returnStatus);
		
		System.exit(returnStatus);
	}



}
