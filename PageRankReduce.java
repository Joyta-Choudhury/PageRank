//jchoudh1@uncc.edu
//Joyta Choudhury

package com.joyta;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//The reducer which  is used to calculate the page ranks
	public class PageRankReduce extends Reducer<Text, Text, Text, Text> {
		private Text final_key = new Text();
		public static final double damping_factor = 0.85;
		
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
					
			StringBuffer sb = new StringBuffer();
			double page_rank = 0.0;
			
			try {
				
				for (Text value : values) {
					
					if (value.toString().contains("#####") && value.toString().length() > 1) {//check for list of links or nodes
						sb.append(value.toString());
						final_key.set(key);
					} else {
						if (value.toString().length() > 1)
							
							//page rank of the nodes
							page_rank += Double.parseDouble(value.toString().trim());
					}
				}
				//formula for calculating the page rank
				page_rank = (1 - damping_factor) + (damping_factor * page_rank);
				
				//writes the updated pagerank with list of values
				context.write(new Text(final_key + "||" + page_rank + "!@!"), new Text(sb.toString().trim()));
				
			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}
	
