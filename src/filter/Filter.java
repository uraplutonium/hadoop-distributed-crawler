package filter;

import hdcrawler.HDCrawler;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 进行URL外链过滤的map-reduce任务
 * @author Felix.Ting
 *
 * @see FilterMapper
 * @see FilterReducer
 */
public class Filter {

	public void filter() {
		Configuration conf = new Configuration();
		String[] param = new String[4];
		param[0] = HDCrawler.workspace + "extendedURL";
		param[1] = HDCrawler.workspace + "URLPoolCopy";
		param[2] = HDCrawler.workspace + "repeatedURL";
		param[3] = HDCrawler.workspace + "tempURLPool";
		String[] hargs = new GenericOptionsParser(conf, param).getRemainingArgs();
		
		try {
			Job job = new Job(conf, "FilterJob");
			System.out.println("Setting filter job...");
	
			job.setJarByClass(Filter.class);
			
			job.setMapperClass(FilterMapper.class);
			job.setReducerClass(FilterReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(hargs[0]));
			System.out.println("Add InputPath:" + hargs[0]);
			FileInputFormat.addInputPath(job, new Path(hargs[1]));
			System.out.println("Add InputPath:" + hargs[1]);
			
			Configuration hdfsconf = new Configuration();		
			Path reURLPath = new Path(HDCrawler.workspace + "tempURLPool");
			try {
				FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), hdfsconf);
				
				boolean exist = hdfs.exists(reURLPath);
				if(exist) {		// 如果临时URL池存在，则添加到输入路径中
					FileInputFormat.addInputPath(job, new Path(hargs[2]));
					System.out.println("Add InputPath:" + hargs[2]);
				}
			}
			catch(IOException exc) {
				exc.printStackTrace();
			}
			
			FileOutputFormat.setOutputPath(job, new Path(hargs[3]));
			System.out.println("Set OutputPath:" + hargs[3]);

			System.out.println("Start filtering...");
			job.waitForCompletion(true);
			System.out.println("Filter done!");
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