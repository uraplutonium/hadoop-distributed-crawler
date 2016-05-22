package eliminator;

/**
 * 消重器接口：用于判断某URL是否与某已扫描的URL重复
 * @author Felix.Ting
 */
public interface Eliminator {
	
	/**
	 * 取得消重器的名称，用于算法类管理器返回算法类实例
	 * @return 消重器的名称
	 */
	String getName();
	
	/**
	 * 判断某URL是否与某已扫描的URL重复
	 * @param url 需判断的URL
	 * @param scannedURL 已扫描的URL
	 * @return 若重复，则返回true，否则返回false
	 */
	boolean isRepeated(String url, String scannedURL);
	
}