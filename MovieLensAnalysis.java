package movielens;

//This Analysis determines the maximum no of users for a movie

import java.io.IOException;
import java.util.*;

//All these packages are present in hadoop-common.jar
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
//All these packages are present in hadoop-mapreduce-client-core.jar
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MovieLensAnalysis {
	public class MovieLensConstants {
		public static final int USERID = 0;
		public static final int MOVIEID = 1;
		public static final int RATING = 2;
		public static final int TIMESTAMP = 3;		
		
	}
	
	public static class MovieLensMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
		
		IntWritable userid = new IntWritable();
		IntWritable movieid = new IntWritable();
	
		public void map(Object key,Text value,Mapper<Object,Text,IntWritable,IntWritable>.Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			userid.set(Integer.parseInt(parts[MovieLensConstants.USERID]));
			movieid.set(Integer.parseInt(parts[MovieLensConstants.MOVIEID]));
			context.write(movieid,userid);
		}
	}
	public static class MovieLensReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
			public void reduce(IntWritable movieid,Iterable<IntWritable> userid, Reducer<IntWritable,IntWritable,IntWritable,IntWritable>.Context context) throws IOException, InterruptedException{
				Set<Integer> userIdSet = new HashSet<Integer>();
				for(IntWritable user : userid){
					userIdSet.add(user.get());
				}
				IntWritable size = new IntWritable(userIdSet.size());
				context.write(movieid,size);
			}
	}
	
	public static void main (String[] args) throws IOException, Exception, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"MovieLensAnalysis");
		
		job .setJarByClass(MovieLensAnalysis.class);
		job.setMapperClass(MovieLensMapper.class);
		job.setReducerClass(MovieLensReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath,true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
		
