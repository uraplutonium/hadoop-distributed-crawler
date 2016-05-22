package selector;

import java.util.Map;
import java.util.Set;

/**
 * 选择器接口：用于从URL池中选择要处理的URL
 * @author Felix.Ting
 */
public interface Selector {
	
	/**
	 * 取得选择器的名称，用于算法类管理器返回算法类实例
	 * @return 选择器的名称
	 */
	String getName();
	
	/**
	 * 从URL池中挑选某些URL，作为即将处理的URL
	 * @param URLPool URL池
	 * @return 待处理的URL的集合
	 */
	Set<String> getSelectedURL(Map<String, Integer> URLPool);
	
	/**
	 * 取得除了被挑选处理URL的URL集合
	 * @param URLPool URL池
	 * @return 未被挑选的URL的集合
	 */
	Map<String, Integer> getLeftURL(Map<String, Integer> URLPool);
	
}