package minecloseditemsets.app;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StringItemsetList extends ArrayList<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public StringItemsetList(List<String> asList) {
		super(asList);
	}

	public StringItemsetList() {
		super();
	}

	public String toString() {

		String[] array = new String[this.size() * 2];
		Iterator<String> iter = this.iterator();
		for (int i = 0; iter.hasNext(); i += 2) {
			array[i] = iter.next();
			array[i + 1] = " ";
		}
		StringBuffer stringbf = new StringBuffer();
		for (int i = 0; i < array.length - 1; i++) {
			stringbf.append(array[i]);
		}
		return stringbf.toString();
	}

}
