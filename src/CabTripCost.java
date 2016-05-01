import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author Delano Greenidge
 *
 */
public class CabTripCost extends Configured implements Tool {
	
	private static Logger theLogger = Logger.getLogger(CabTripCost.class);

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        conf.set("mapreduce.output.key.field.separator", ",");
        conf.set("mapreduce.textoutputformat.separator", ","); 


		Job job = Job.getInstance(conf, "Cab trip cost calc");

	    FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// used for calculating taxi cost
		conf.setDouble("SFO_lat", 37.62131);
		conf.setDouble("SFO_long", -122.37896);
		conf.setDouble("SFO_range", 1.00);
		conf.setDouble("taxi_start_charge", 3.50);
		conf.setDouble("taxi_charge_per_unit_dist", 1.71);
		
		// disables reduce step
		//job.setNumReduceTasks(0);

		job.setJarByClass(CabTripCost.class);
		job.setJobName("CabTrips ["+args[0]+"]");

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		
		//job.setMapOutputKeyClass(CabTripCostRecord.class);
		//job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(CabTripCostRecord.class);
		job.setOutputValueClass(Text.class);

        job.setMapperClass(CabTripCostMapper.class);
        job.setReducerClass(CabTripCostReducer.class); 
    	job.setPartitionerClass(CabTripCostRecordPartitioner.class);
    	job.setGroupingComparatorClass(CabTripCostRecordComparator.class);

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
		int returnStatus = ToolRunner.run(new CabTripCost(), args);
		return returnStatus;
	}	
		
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// Make sure there are exactly 2 parameters
		if (args.length < 2) {
			theLogger.warn("CabTripCost <input-file> <output-dir> [<num-reduce-tasks>]");
			throw new IllegalArgumentException("Usage: CabTrips <input-dir> <output-dir> [<num-reduce-tasks>]");
		}

		int returnStatus = submitJob(args);
		theLogger.info("returnStatus="+returnStatus);
		
		System.exit(returnStatus);
	}

}
