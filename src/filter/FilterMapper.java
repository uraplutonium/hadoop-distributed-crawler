package filter;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * URL外链过滤Mapper
 * @author Felix.Ting
 *
 * @see Mapper
 */
public class FilterMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	@Override
	public void map(Object num, Text extURLRecord, Context context) {
		String url;
		IntWritable assessment;
		StringTokenizer token = new StringTokenizer(extURLRecord.toString());
		url = token.nextToken();
		assessment = new IntWritable(Integer.valueOf(token.nextToken()));
		try {
			context.write(new Text(url), assessment);
		}
		catch(InterruptedException exc) {
			exc.printStackTrace();
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
}