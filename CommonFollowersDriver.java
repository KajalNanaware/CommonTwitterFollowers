
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class CommonFollowersDriver  {
public static void main(String[] args) throws Exception {
	if (args.length <2) {
	      System.err.println("Social Graph Analysis");
	      System.exit(-1);
	    }
	//Create temporary file to store result of job1
	Configuration conf=new Configuration();
	Job job1= Job.getInstance(conf, "common followers");
	job1.setJarByClass(CommonFollowersDriver.class);
	
	//Setting the input and output paths.
	FileInputFormat.addInputPath(job1, new Path(args[0]));
	Path tempout = new Path("temp");
	SequenceFileOutputFormat.setOutputPath(job1, tempout);
	job1.setOutputFormatClass(SequenceFileOutputFormat.class);	
	
	//Setting the mapper, reducer classes for job1
	job1.setMapperClass(CommonFollowersMapper.class);
	job1.setReducerClass(CommonFollowersReducer.class);
	job1.setMapOutputKeyClass(Text.class);
	job1.setMapOutputValueClass(Text.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(Text.class);
	job1.waitForCompletion(true);
	
	//Job2 is mapreduce job for second step 
	Job job2 = Job.getInstance(conf,"common followers");
	job2.setJarByClass(CommonFollowersDriver.class);
	
	//The input of job2 is output of job1
	job2.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job2, tempout);
	FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	
	job2.setReducerClass(CommonFollowersReducer2.class);
	job2.setMapOutputKeyClass(Text.class);
	job2.setMapOutputValueClass(Text.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	job2.waitForCompletion(true);
	
	//Submit the job and wait for its completion
	System.exit(job2.waitForCompletion(true) ? 0 : 1);
	
}
}
