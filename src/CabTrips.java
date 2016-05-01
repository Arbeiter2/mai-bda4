import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * @author Delano Greenidge
 *
 */
public class CabTrips extends Configured implements Tool{
	
	private static Logger theLogger = Logger.getLogger(CabTrips.class);

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Cab trip builder");
	    FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// use user-supplied number of reduce tasks
		int numReduceTasks = 1;
		if (args.length > 2)
		{
			numReduceTasks = Integer.parseInt(args[2]);
			if (numReduceTasks > 1)
				job.setNumReduceTasks(numReduceTasks);
		}
		job.setJarByClass(CabTrips.class);
		job.setJobName("CabTrips ["+args[0]+"], R"+Integer.toString(numReduceTasks));

		job.setOutputKeyClass(VehicleIDTimestamp.class);
		job.setOutputValueClass(CabTripSegment.class);
		
    	job.setMapperClass(CabTripMapper.class);
    	job.setReducerClass(CabTripReducer.class);		
    	job.setPartitionerClass(VehicleIDTimestampPartitioner.class);
    	job.setGroupingComparatorClass(VehicleIDTimestampComparator.class);

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
