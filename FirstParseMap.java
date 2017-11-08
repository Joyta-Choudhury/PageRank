//jchoudh1@uncc.edu
//Joyta Choudhury

package com.joyta;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//This map is used to parse the input text file using regex
	public class FirstParseMap extends Mapper<Object, Text, Text, Text> {
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringBuffer sb = new StringBuffer();
			//line by line value
			String line_text = value.toString();
			String page_title = null;
			String text_tags = null;
			String links = null;
			
			//if there is some text in the line
			if (!line_text.isEmpty()) {
				
				// matches pattern for the title
				Pattern title_pat = Pattern.compile("<title>(.*?)</title>");    //gets the title patterns
				Matcher title_mat = title_pat.matcher(line_text);               //matches the pattern with the line
				while (title_mat.find()) {
					page_title = title_mat.group(1).trim();                    //it contains the title of the page
				}
				
				// matches pattern for the nodes in text
				Pattern text_pat = Pattern.compile("<text(.*?)>(.*?)</text>");
				Matcher text_mat = text_pat.matcher(line_text);
				
				while (text_mat.find()) {
					text_tags = text_mat.group(2);                             //the text inside the tags <text> and </text>
				}
				if (text_tags != null) {
					Pattern pattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
					Matcher matcher = pattern.matcher(text_tags);
					
					while (matcher.find()) {
						links = matcher.group().replace("[[", "").replace("]]", "");
						sb.append(links + "#####"); //stringBuilder contains the links
						//sb.append(matcher.group(1) + "#####");
					}
				}
				context.write(new Text(page_title), new Text(sb.toString()));
			}
		}
	}
