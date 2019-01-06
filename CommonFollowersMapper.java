import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.json.JSONObject;
import org.apache.hadoop.io.*;

public class CommonFollowersMapper extends Mapper<LongWritable, Text, Text, Text> {
	
  public void map(LongWritable key, Text value, Context context)
		  throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  	  
	  String[] followers = line.split(" ");
	  if(followers.length>=2)		  
		  context.write(new Text(followers[0]), new Text(followers[1]));	  
    }
  }

