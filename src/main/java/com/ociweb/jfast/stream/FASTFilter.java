package com.ociweb.jfast.stream;

public class FASTFilter {

	public static final FASTFilter none = new FASTFilter() {
		byte go(int newGroupPos, FASTRingBuffer ringBuffer) {
			return 1;
		}
	};
	
	final FASTFilter next;
	
	private FASTFilter() {
		this.next = null;
	}
	
	public FASTFilter(FASTFilter next) {
		this.next = next;
	}
	
	//TODO: B, filter, can accept or reject once based on first fragment only
	
	
	byte defaultBehavior() {
		return 1;
	}
	
	//returns 1  for normal keep behavior
	//returns 0  for undetermined hold behavior (assumed omnipotent until determined)
	//return  -1 for filter remove behavior
	byte go(int newGroupPos, FASTRingBuffer ringBuffer) {
		
		//
		return next.go(newGroupPos,ringBuffer);
	}
	
}
