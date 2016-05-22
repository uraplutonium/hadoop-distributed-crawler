package extender;

import hdcrawler.HDCrawler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 进行URL外链扩展的map-reduce任务
 * @author Felix.Ting
 *
 * @see ExtendMapper
 * @see ExtendReducer
 */
public class Extender {

	public void extend() {
		Configuration conf = new Configuration();
		String[] param = new String[2];
		param[0] = HDCrawler.workspace + "URLInfo";
		param[1] = HDCrawler.workspace + "extendedURL";
		String[] hargs = new GenericOptionsParser(conf, param).getRemainingArgs();
		
		try {
			Job job = new Job(conf, "ExtenderJob");
			System.out.println("Setting extend job...");
	
			job.setJarByClass(Extender.class);
			
			job.setMapperClass(ExtendMapper.class);
			job.setReducerClass(ExtendReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(hargs[0]));
			System.out.println("Add InputPath:" + hargs[0]);
			FileOutputFormat.setOutputPath(job, new Path(hargs[1]));
			System.out.println("Set OutputPath:" + hargs[1]);

			System.out.println("Start extending...");
			job.waitForCompletion(true);
			System.out.println("Extend done!");
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
		catch(ClassNotFoundException exc) {
			exc.printStackTrace();
		}
		catch(InterruptedException exc) {
			exc.printStackTrace();
		}
	}
	
}