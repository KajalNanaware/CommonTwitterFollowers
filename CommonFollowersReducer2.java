import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommonFollowersReducer2 extends Reducer<Text, Text, Text, Text> {
	
	  public void reduce(Text key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException {	  
	    
		String allFollowers = "";
		for(Text value: values) {
			allFollowers += value.toString()+",";
		}
		String followers = allFollowers.substring(0, (allFollowers.length()-1)); 
		
		context.write(key, new Text("{"+followers+"}"));
	  }
	}