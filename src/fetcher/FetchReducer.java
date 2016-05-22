package fetcher;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * URL信息抓取Reducer
 * @author Felix.Ting
 *
 * @see Reducer
 * @see URLInfoWritable
 */
public class FetchReducer extends Reducer<URLInfoWritable, NullWritable, URLInfoWritable, NullWritable> {

	@Override
	public void reduce(URLInfoWritable info, Iterable<NullWritable> nullvalue, Context context) {
		try {
			context.write(info, NullWritable.get());
		}
		catch(InterruptedException exc) {
			exc.printStackTrace();
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
}