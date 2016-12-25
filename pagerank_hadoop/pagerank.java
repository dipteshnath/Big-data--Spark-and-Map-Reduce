package org.myorg;
import java.net.URI;
import java.util.Collection;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class wordcount extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new wordcount(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {
		
		int i = 0;
		
		// JOb1 to get title and link
		Job job = Job.getInstance(getConf(), " wordcount ");
		//Configuration conf = new Configuration();
		//Job job = new Job(conf);
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);//GIve the input path as argument
		FileOutputFormat.setOutputPath(job, new Path(args[1]+i));
		//job.setJobName("textparser");
		job.setMapperClass(PageMap1.class);
		job.setReducerClass(PageReduce1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean res = false;
		
		// Job 2 to calculate page rank as per iterations
		if (job.waitForCompletion(true)) {
			Job job2 = Job.getInstance(getConf(), " wordcount ");
			job2.setJarByClass(this.getClass());
			FileInputFormat.addInputPaths(job2, args[1] + i);
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+ (i + 1)));
			job2.setMapperClass(PageMap2.class);
			job2.setReducerClass(PageReduce2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			//job.setJobName("rankcalculator");
			res = job2.waitForCompletion(true) ? true :false;
			
			for (i=1; i <10 ; i++) {//Here the number of iteration is defined as 10
			  job2 = Job.getInstance(getConf(), " wordcount ");
				job2.setJarByClass(this.getClass());
				FileInputFormat.addInputPaths(job2, args[1] + i);
				FileOutputFormat.setOutputPath(job2, new Path(args[1]+ (i + 1)));
				job2.setMapperClass(PageMap2.class);
				job2.setReducerClass(PageReduce2.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				res = job2.waitForCompletion(true) ? true : false;
			}
			
		}
		//Job 3 for page rank
		if(res==true){
		Job job3 = Job.getInstance(getConf(), " wordcount ");
		job3.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job3, args[1] +i);
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"-sort"));//folder gives the final sorted pagerank document
		job3.setMapperClass(PageMap3.class);
		job3.setReducerClass(PageReduce3.class);
		job3.setNumReduceTasks(1);
		//job.setJobName("sorter");
		job3.setOutputKeyClass(DoubleWritable.class);
		job3.setOutputValueClass(Text.class);
		// sort in descending order
		job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		return job3.waitForCompletion(true) ? 0 : 1;
		}
		
		return 0;
		
	}

	// Mapper of the job1-Finds title + linked pages

	public static class PageMap1 extends Mapper<LongWritable, Text, Text, Text> {

		//private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {		
			String line = lineText.toString();
			String STARTTAG="<title>";
			String STOPTAG="</title>";
			String STARTLINK="<text";
			String STOPLINK="</text>";
			Text currentWord  = new Text();
			try
			{
			if (line != null && !line.trim().equals("")) {
				
				String title = StringUtils.substringBetween(line, STARTTAG, STOPTAG);//Take string between title tags				
				String lnk[] = StringUtils.substringsBetween(StringUtils.substringBetween(line, STARTLINK, STOPLINK), "[[", "]]");//take string between brackets
				for (int i = 0; i < lnk.length; i++) {//loop to create key value pair of title and the text inside the bracket
					context.write(new Text(title.trim()), new Text(lnk[i].trim()));
				}

			}
			}
			catch(Exception e){return;}
			
		}

	}
	
	public static class PageReduce1 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text title, Iterable<Text> link, Context context) throws IOException, InterruptedException {
			
			boolean first = true;
			String rank = "1.0 \t";
			//To find the same titles in the given text
			for (Text t : link) {
				if (!first) {
					rank = rank + "---";
				}
				rank = rank + t.toString();
				first = false;
			}

			context.write(title, new Text(rank));
		}

	}

	//Counts the total outgoing links
		public static class PageMap2 extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();
			int pind = line.indexOf("\t");
			int rind = line.indexOf("\t", pind + 1);

			if(pind == -1||rind == -1 ){
				return;
			}
			
			
			String page = line.substring(0, pind);		
			String rank = line.substring(0, rind + 1);
			
			context.write(new Text(page), new Text("!"));
			if (rind == -1)return;
			String links = line.substring(rind + 1, line.length());
			links = links.trim();
			String link[] = links.split("---");
			int total_links = link.length;
// get outgoing pages 
		for (int i = 0; i < link.length; i++) {
				String value = rank + total_links;
				context.write(new Text(link[i]), new Text(value));
			}
			
			context.write(new Text(page), new Text("$" + links));
		}

	}

	public static class PageReduce2 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text title, Iterable<Text> link, Context context) throws IOException, InterruptedException {

			double oPageRanks = 0.0;
			boolean samepage = false;
			String links = "";
		    float X = 0.85f;//Store the damping

			for (Text li : link) {
				String pageRank = li.toString();

				if (pageRank.equals("!")) {
					samepage = true;
					continue;
				}

				if (pageRank.startsWith("$")) {
					links = "\t" + pageRank.substring(1);
					continue;
				}

				String split[] = pageRank.split("\\t");
				if(split[1].equals(""))return;
				if(split[2].equals(""))return;
				double rank = Double.valueOf(split[1]);
				int outLink = Integer.valueOf(split[2]);
				oPageRanks =oPageRanks+ (rank / outLink);
			}
			if (samepage==false)
				return;

			//Page rank update
			Double updateRank =  (X * oPageRanks + (1 - X));
			context.write(title, new Text(updateRank + links));
		}
	}

	//Sort in descending order
	public static class PageMap3 extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			
			try{
			String line = lineText.toString();
			ArrayList p = new ArrayList();			
			p.add(line.substring(0, line.indexOf("\t")));
			p.add(line.substring(line.indexOf("\t")+1,line.lastIndexOf("\t")));
			
			if(p.get(0).equals("")) return;
			if(p.get(1).equals("")) return;
			String a=(String)p.get(0);
			String b=(String)p.get(1);

			Text passv = new Text(a);
			DoubleWritable passk = new DoubleWritable(Double.parseDouble(b));
				
			
			context.write(passk,passv);
			}catch(Exception e){
				return;
			}
							
		}

	}
	//To store the final sorted output in Text<tab>Score format
	public static class PageReduce3 extends Reducer<DoubleWritable, Text,Text , DoubleWritable> {

		public void reduce(DoubleWritable name, Iterable<Text> link, Context context) throws IOException, InterruptedException {

			for(Text lnk : link){
				context.write(new Text(lnk),name );
			}
			
		}

	}
	

}