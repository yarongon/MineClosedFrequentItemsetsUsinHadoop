package minecloseditemsets.app;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Vector;

public class ItemsetHashTable extends Hashtable<String, Vector<Integer>> {

	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public String toString(){
		Hashtable<String, Integer> outputMap = new Hashtable<String, Integer>();
		Iterator<Entry<String, Vector<Integer>>> entries = this.entrySet().iterator();	
		while(entries.hasNext()){
			Entry<String, Vector<Integer>> thisEntry = entries.next();
			String key = thisEntry.getKey();
			Vector<Integer> value = thisEntry.getValue();
			outputMap.put(key, value.size());
		}
		return outputMap.toString();
	}





}
