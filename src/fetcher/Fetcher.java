package fetcher;

import hdcrawler.HDCrawler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 进行URL信息抓取的map-reduce任务
 * @author Felix.Ting
 * 
 * @see FetchMapper
 * @see FetchReducer
 */
public class Fetcher {
	
	public void fetch() {
		Configuration conf = new Configuration();
		String[] param = new String[2];
		param[0] = HDCrawler.workspace + "selectedURL";
		param[1] = HDCrawler.workspace + "URLInfo";
		String[] hargs = new GenericOptionsParser(conf, param).getRemainingArgs();
		
		try {
			Job job = new Job(conf, "FetcherJob");
			System.out.println("Setting fetch job...");
	
			job.setJarByClass(Fetcher.class);
			
			job.setMapperClass(FetchMapper.class);
			job.setReducerClass(FetchReducer.class);
			
			job.setMapOutputKeyClass(URLInfoWritable.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputKeyClass(URLInfoWritable.class);
			job.setOutputValueClass(NullWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(hargs[0]));
			System.out.println("Add InputPath:" + hargs[0]);
			FileOutputFormat.setOutputPath(job, new Path(hargs[1]));
			System.out.println("Set OutputPath:" + hargs[1]);
			
			System.out.println("Start fetching...");
			job.waitForCompletion(true);
			System.out.println("Fetch done!");
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