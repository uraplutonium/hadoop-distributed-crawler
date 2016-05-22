package selector;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * SelectTopTen选择器：选择URL池中相关度估计值最大的10个URL
 * @author Felix.Ting
 *
 * @see Selector
 * @see AsmURLPair
 */
public class SelectTopTen implements Selector {

	@Override
	public String getName() {
		return "SelectTopTen";
	}
	
	@Override
	public Map<String, Integer> getLeftURL(Map<String, Integer> URLPool) {
		List<AsmURLPair> pairList = new LinkedList<AsmURLPair>();
		for(String url : URLPool.keySet()) {
			AsmURLPair pair = new AsmURLPair(URLPool.get(url), url);
			pairList.add(pair);
		}
		
		Collections.sort(pairList);	// 按照相关度估计值进行排序
		
		Map<String, Integer> URLPoolCopy = new HashMap<String, Integer>();
		int i;
		for(i = 0 ; i < 10 && pairList.iterator().hasNext(); i++) {
			pairList.remove(0);
		}
		
		for(AsmURLPair pair : pairList) {
			URLPoolCopy.put(pair.getURL(), pair.getAssessment());
		}
		
		return URLPoolCopy;
	}

	@Override
	public Set<String> getSelectedURL(Map<String, Integer> URLPool) {
		List<AsmURLPair> pairList = new LinkedList<AsmURLPair>();
		for(String url : URLPool.keySet()) {
			AsmURLPair pair = new AsmURLPair(URLPool.get(url), url);
			pairList.add(pair);
		}
		
		Collections.sort(pairList);		// 按照相关度估计值进行排序
		
		Set<String> selectedURL = new TreeSet<String>();
		int i;
		for(i = 0 ; i < 10 && pairList.iterator().hasNext(); i++) {
			AsmURLPair pair = pairList.get(0);
			pairList.remove(0);
			System.out.println("#selected:" + pair.getURL());
			selectedURL.add(pair.getURL());
		}
		
		return selectedURL;
	}

}