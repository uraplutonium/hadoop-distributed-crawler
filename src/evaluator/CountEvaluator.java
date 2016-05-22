package evaluator;

import java.util.Map;
import java.util.Set;

import fetcher.URLInfoWritable;

/**
 * CountEvaluator评估器：将网页中某关键词出现的个数设定为其相关度
 * @author Felix.Ting
 *
 * @see Evaluator
 */
public class CountEvaluator implements Evaluator {

	@Override
	public String getName() {
		return "CountEvaluator";
	}

	@Override
	public int getRank(URLInfoWritable urlInfo, String keyword) {
		int amount;
		Map<String, Set<Integer>> urlLocation = urlInfo.getLocationMap();
		if(urlLocation.containsKey(keyword)) {
			amount = urlLocation.get(keyword).size();
		}
		else {
			amount = 0;
		}
		return amount;
	}

	@Override
	public int getRank(URLInfoWritable urlInfo, Set<String> keywords) {
		int amount = 0;
		Map<String, Set<Integer>> urlLocation = urlInfo.getLocationMap();
		
		for(String keyword : keywords) {
			if(urlLocation.containsKey(keyword)) {
				amount += urlLocation.get(keyword).size();
			}
		}
		
		return amount;
	}

}