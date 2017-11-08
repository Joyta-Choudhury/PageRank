//jchoudh1@uncc.edu
//Joyta Choudhury

package com.joyta;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FirstParseReduce extends Reducer<Text, Text, Text, Text> {
	
	double value_of_n;
	
	// to get the value of n
	public void setup(Context context) throws IOException, InterruptedException{
		
		value_of_n = context.getConfiguration().getDouble("VALUE_OF_N", 1);
		
	}
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
				
		StringBuffer sb = new StringBuffer();
		
		try { 
			
			for (Text value : values) {
				
				if (value.toString().contains("#####") && value.toString().length() > 1) {   //check for list of pages or links
					sb.append(value.toString());     //using string buffer to append
				}
			}
			
			//initial page rank for each node
			double one_by_n = (double)1/(value_of_n);
			
			//writes the updated pagerank with list of values
			//title||value_of_n!@! link1##### link2##### and so on
			context.write(new Text(key + "||" + one_by_n + "!@!"), new Text(sb.toString().trim())); 
																
		
		} 
		catch (Exception e) {
			System.out.println(e);
		}
	}
}
