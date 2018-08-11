package com.ociweb.pronghorn.stage.scheduling;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.Map;

public class MemoryLeakDetector {

	private long[] dataStorage;
	private int lastPosition;
	private final int sizeOfUnit = 4;
		
	public MemoryLeakDetector(int maxHeads) {
		
		dataStorage = new long[sizeOfUnit*(1+maxHeads)];
		lastPosition = dataStorage.length;		
		
	}
	
	public void ensureSpace(int idx) {
		
		if (idx>=lastPosition) {
			long[] newArray = new long[idx*2];
			System.arraycopy(dataStorage, 0, newArray, 0, dataStorage.length);
			dataStorage = newArray;
		}
		
	}
	
	///////////////////////
	////read write methods
	//////////////////////
	//sum of all pointers to heap (64 bits)
	//jump to children list (32 bits)
	//count of children (32 bits)
	//
	
	
	
	///////////////////////
	///////////////////////
	
	
	/**
	 * Called once on every independent object in this round
	 * @param obj
	 */
	public void updateData(Object obj, int id) {
		
		
		int idx = (sizeOfUnit*id);
		
		Class clazz = obj.getClass();		
		Field[] fields = clazz.getDeclaredFields();
		
		IdentityHashMap<Object, Object> dupSet = new IdentityHashMap<Object, Object>();
		
		
		int f = fields.length;
		while (--f>=0) {
			fields[f].setAccessible(true);
			try {
				Object child = fields[f].get(obj);
				//TODO: later we need to only record those which have changed.
				
				dupSet.clear();
				int count = recursiveScan(child, dupSet, idx);

				//if we are above the old total then record to find it?
				//setNewTotal(idx, count);
				
			} catch (IllegalArgumentException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			
			
			
			
		}
		
	}
	
	private int recursiveScan(Object obj, Map<Object,Object> dupSet, int idx) {
	
		//TODO: must allocate working space to be held in case of memory error.
		//TODO: can find uncontrolled increases in values
		//TODO: if use use bloom filters could detect high rates of object churn.
		
		
		
		//if that position indicates a possible leak then expand it one new level
		//boolean isGrowing = isPossibleLeak(idx);
		//we will narrow to the leak on following calls.
		
		int total = 0;
		if (!dupSet.containsKey(obj)) {
			dupSet.put(obj, obj);
	
			Class clazz = obj.getClass();
				
			if (clazz.isArray()) {
				int length = Array.getLength(obj);
				total+=length;
				if (clazz.getComponentType().isPrimitive()) {
					//leak could only happen if this array grows in length.
				} else {					
					int w = length;
					while (--w>=0) {
						Object item = Array.get(obj, w);
						total++;//count each object reference only once				
						int count = recursiveScan(item, dupSet, idx);
						total += count;
					}
					return total;
				}
				return total;
			}
			if (clazz.isPrimitive()) {
				//we are only counting things which could cause a leak
				return 0;
			}		
						
			Field[] fields = clazz.getDeclaredFields();
			
			int f = fields.length;
			while (--f>=0) {				
				fields[f].setAccessible(true);
				try {
 					 Object child = fields[f].get(obj);
					 total++;//count each object reference only once				
					 int count = recursiveScan(child, dupSet, idx);
					 total += count;				
					
				} catch (IllegalArgumentException e) {
					throw new RuntimeException(e);
				} catch (IllegalAccessException e) {
					throw new RuntimeException(e);
				}				
			}
		}
		return total;
	}

	public void finishRound() {
		
		//??
	}
	
}
