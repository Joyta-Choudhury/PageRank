//jchoudh1@uncc.edu
//Joyta Choudhury

package com.joyta;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//Mapper for calculating page rank
public class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text link = new Text();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {//title||0.15\001 file1### file2###

		if (value.toString().isEmpty() == false || value.toString() != null) {
			
			String[] title_rank_links = value.toString().split("!@!");    //splits title+pagerank and list
			String[] title_rank= title_rank_links[0].split("\\|\\|");     //splits title and the page rank
			
			if (title_rank_links[1].trim().length() > 0) {                //if links exists
				
				//StringTokenizer contains all the outgoing links of a page
				StringTokenizer string_token = new StringTokenizer(title_rank_links[1], "#####");
				
				
				//to get the count of the links
				int count = string_token.countTokens();
				
				while (string_token.hasMoreTokens()) {
					
					//calculating new page rank based on links count
					double page_rank = (Double.parseDouble(title_rank[1]) / new Double(count));
					//set the link with the links one by one
					link.set(string_token.nextToken().trim());
					
					if (page_rank != 0.0) {
						
						context.write(link, new Text(page_rank + ""));//link and its initial page rank
					}
				}
			}
			context.write(new Text(title_rank[0]), new Text(title_rank_links[1]));//title and the list of files
		}
		
	}
}
