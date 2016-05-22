package fetcher;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * URL信息抓取Mapper
 * @author Felix.Ting
 * 
 * @see Mapper
 * @see URLAnalyzer
 * @see URLInfoWritable
 */
public class FetchMapper extends Mapper<Object, Text, URLInfoWritable, NullWritable> {

	@Override
	public void map(Object num, Text selectedURL, Context context) {
		System.out.println("selectedURL: " + selectedURL);
		
		URLAnalyzer analyzer = new URLAnalyzer();
		URLInfoWritable urlInfo = analyzer.getURLInfo(selectedURL.toString());	// 使用URL分析器得到URL信息
		
		try {
			context.write(urlInfo, NullWritable.get());
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
		catch(InterruptedException exc) {
			exc.printStackTrace();
		}
	}
	
}