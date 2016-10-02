package cn.smq.mr;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HttpDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		// check arguments 
		if (args.length != 2) {
			System.out.println("Usage: <inputPath> <outputPath>");
			return -1;		
		}
		
		//arguments qualify
		Configuration conf = getConf();
		Job job = new Job(conf);
		
		/*
		 * basic configuration
		 */
		job.setJobName("http demo");
		job.setJarByClass(HttpDriver.class);
		job.setMapperClass(HttpMapper.class);
		job.setReducerClass(HttpReducer.class);
		
		
		//set Map output type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(HttpRecord.class);
		
		//set reducer output type
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		//set output path, input path
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
		
		
		return status == true ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		// start MapReduce
		
		int runStatus = ToolRunner.run(new HttpDriver(), args);
		System.exit(runStatus);
	}
}
