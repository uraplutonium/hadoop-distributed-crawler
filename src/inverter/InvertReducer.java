package inverter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * URL信息倒排Reducer
 * @author Felix.Ting
 *
 * @see Reducer
 * @see URLRankWritable
 */
public class InvertReducer extends Reducer<Text, URLRankWritable, Text, URLRankWritable> {

	@Override
	public void reduce(Text keyword, Iterable<URLRankWritable> urlRanks, Context context) {
		// 将关键词keyword所有的URLRank合并为一个URLRank
		URLRankWritable urlRank = new URLRankWritable();
		for(URLRankWritable ur : urlRanks) {
			for(String url : ur.keySet()) {
				urlRank.put(url, ur.get(url));
			}
		}
		
		try {
			context.write(keyword, urlRank);
		}
		catch(InterruptedException exc) {
			exc.printStackTrace();
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
}