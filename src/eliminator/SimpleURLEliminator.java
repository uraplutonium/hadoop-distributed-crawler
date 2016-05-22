package eliminator;

/**
 * SimpleURLEliminator消重器：通过两个网页的URL来判断是否相同
 * @author Felix.Ting
 *
 * @see Eliminator
 */
public class SimpleURLEliminator implements Eliminator {

	@Override
	public String getName() {
		return "SimpleURLEliminator";
	}

	@Override
	public boolean isRepeated(String url, String scannedURL) {
		return url.equals(scannedURL);
	}

}