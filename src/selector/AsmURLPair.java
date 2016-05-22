package selector;

/**
 * 估计值-URL对
 * @author Felix.Ting
 * @see Comparable
 */
public class AsmURLPair implements Comparable<AsmURLPair> {
	
	int assessment;	// URL与主题相关度的估计值
	String url;		// URL
	
	public AsmURLPair(int asm, String url) {
		assessment = asm;
		this.url = new String(url);
	}

	public int getAssessment() {
		return assessment;
	}
	
	public String getURL() {
		return url;
	}
	
	@Override
	public int compareTo(AsmURLPair thatPair) {
		return (assessment > thatPair.assessment ? -1 : (assessment < thatPair.assessment ? 1 : 0));
	}

}