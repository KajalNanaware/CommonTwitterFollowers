import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class CommonFollowersReducer extends Reducer<Text, Text, Text, Text> {
	
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	 String s1 = "";
	 String s2 = "";
	 
	 ArrayList<String> a = new ArrayList<String>();
	 
	 for(Text value: values) {
		 a.add(value.toString());
	 }
	 
	 for(String a1: a) {
		 s1= a1;		 
		 for(String b1:a) {
			 if(!b1.equals(s1) && a.indexOf(b1)>a.indexOf(s1)) {
				 s2 = b1;
				 context.write(new Text(s1+" "+s2), key);				 
			 }
		 }	 
	  } 
	}
}
 