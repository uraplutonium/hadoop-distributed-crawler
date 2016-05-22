package selector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * SelectAll选择器：选择URL池中所有的URL
 * @author uraplutonium
 *
 * @see Selector
 */
public class SelectAll implements Selector {

	@Override
	public String getName() {
		return "SelectAll";
	}

	@Override
	public Set<String> getSelectedURL(Map<String, Integer> URLPool) {
		Set<String> selectedURL = new TreeSet<String>(URLPool.keySet());
		return selectedURL;
	}

	@Override
	public Map<String, Integer> getLeftURL(Map<String, Integer> URLPool) {
		Map<String, Integer> URLPoolCopy = new HashMap<String, Integer>();
		return URLPoolCopy;
	}
}