package extender;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * URL外链扩展Reducer
 * @author Felix.Ting
 *
 * @see Reducer
 */
public class ExtendReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text legalURL, Iterable<IntWritable> assessments, Context context) {
		try {
			context.write(legalURL, assessments.iterator().next());	// 由于对于某一URL与主题的相关度估计值应当一样，所以选择第一个估计值输出
		}
		catch(InterruptedException exc) {
			exc.printStackTrace();
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
}