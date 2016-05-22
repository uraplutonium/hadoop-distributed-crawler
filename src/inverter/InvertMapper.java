package inverter;

import hdcrawler.HDCrawler;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fetcher.URLInfoWritable;

/**
 * URL信息倒排Mapper
 * @author Felix.Ting
 * 
 * @see Mapper
 * @see URLRankWritable
 */
public class InvertMapper extends Mapper<Object, Text, Text, URLRankWritable> {
	
	/**
	 * 建立存储了一个URL信息的URLInfoWritable对象
	 * @param token 字符串标记生成器
	 * @param url 网页的URL
	 * @return 存储了该URL信息的URLInfoWritable对象
	 */
	private URLInfoWritable readInfo(StringTokenizer token, String url) {
		URLInfoWritable info = new URLInfoWritable();
		info.setURL(url);
		
		if(token.hasMoreElements()) {
			int loclen = Integer.valueOf(token.nextToken());
			System.out.println("loclen:" + loclen);
			
			int i;
			for(i = 0 ; i < loclen ; i++) {
				String keyword = token.nextToken();
				int wordlen = Integer.valueOf(token.nextToken());
				int j;
				for(j = 0 ; j < wordlen ; j++) {
					Integer wordloc = Integer.valueOf(token.nextToken());
					info.putURLLocation(keyword, wordloc);
				}
			}
		
			int extlen = Integer.valueOf(token.nextToken());
			for(i = 0 ; i < extlen ; i++) {
				String exturl = token.nextToken();
				info.addExtendedURL(exturl);
			}
		}
		
		return info;
	}
	
	/**
	 * 建立存储了一个关键词所出现的URL及其相关度的HashMap
	 * @param token 字符串标记生成器
	 * @param size 该关键词所出现的URL的数量
	 * @return 存储了一个关键字词所出现的URL及其相关度的URLRank对象
	 */
	private URLRankWritable readURLRank(StringTokenizer token, int size) {
		URLRankWritable urlRank = new URLRankWritable();
		int i;
		for(i = 0 ; i < size ; i++) {
			String url = token.nextToken();
			int rank = Integer.valueOf(token.nextToken());
			urlRank.put(url, rank);
		}
		return urlRank;
	}
	
	@Override
	public void map(Object num, Text urlInfo, Context context) {
		URLRankWritable urlRank;
		
		StringTokenizer token = new StringTokenizer(urlInfo.toString());
		String param = token.nextToken();	// 取得参数，若该数据从倒排表读入，则为关键词,若从URL信息文件读入，则为URL
		int mark = Integer.valueOf(token.nextToken());	// 取得标记参数mark
		if(mark == -1) {	// 若标记参数mark为-1，则说明从URL信息文件读入，按照URLInfoWritable类型解析
			URLInfoWritable info = readInfo(token, param);	// 读入数据，建立URLInfoWritable对象
			
			for(String keyword :info.getLocationMap().keySet()) {
				int rank = HDCrawler.evaluator.getRank(info, keyword);
				urlRank = new URLRankWritable();
				urlRank.put(info.getURL(), rank);
				try {
					System.out.println("key:" + keyword + " UR:" + urlRank.toString());
					context.write(new Text(keyword), urlRank);
				}
				catch(InterruptedException exc) {
					exc.printStackTrace();
				}
				catch(IOException exc) {
					exc.printStackTrace();
				}
			}
		}
		else if(mark >= 0){		// 若标记参数为非负数，则说明从倒排表读入，按照URLRank类型解析
			urlRank = readURLRank(token, mark);		// 读入数据，建立URLRank对象
			try {
				context.write(new Text(param), urlRank);
			}
			catch(InterruptedException exc) {
				exc.printStackTrace();
			}
			catch(IOException exc) {
				exc.printStackTrace();
			}
		}
		else {
			System.out.println("Illegal mark.");
		}
	}

}