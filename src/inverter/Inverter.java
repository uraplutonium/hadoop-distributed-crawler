package inverter;

import hdcrawler.HDCrawler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 进行URL信息倒排的map-reduce任务
 * @author Felix.Ting
 *
 * @see InvertMapper
 * @see InvertReducer
 */
public class Inverter {

	public void invert() {
		Configuration conf = new Configuration();
		String[] param = new String[3];
		param[0] = HDCrawler.workspace + "URLInfo";
		param[1] = HDCrawler.workspace + "tempInvertedTable";
		param[2] = HDCrawler.workspace + "InvertedTable";
		String[] hargs = new GenericOptionsParser(conf, param).getRemainingArgs();
		
		try {
			Job job = new Job(conf, "InverterJob");
			System.out.println("Setting inverte job...");

			job.setJarByClass(Inverter.class);
			
			job.setMapperClass(InvertMapper.class);
			job.setReducerClass(InvertReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(URLRankWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(URLRankWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(hargs[0]));
			System.out.println("Add InputPath:" + hargs[0]);
			FileInputFormat.addInputPath(job, new Path(hargs[1]));
			System.out.println("Add InputPath:" + hargs[1]);
			FileOutputFormat.setOutputPath(job, new Path(hargs[2]));
			System.out.println("Set OutputPath:" + hargs[2]);

			System.out.println("Start inverting...");
			job.waitForCompletion(true);
			System.out.println("Invert done!");
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