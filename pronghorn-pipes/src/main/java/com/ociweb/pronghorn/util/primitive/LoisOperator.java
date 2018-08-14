package com.ociweb.pronghorn.util.primitive;

public abstract class LoisOperator {

	public abstract boolean visit(int idx, LoisVisitor visitor, Lois lois);
	
	public abstract boolean isBefore(int idx, int value, Lois lois);
	
	public abstract boolean isAfter(int idx, int value, Lois lois);
	
	public abstract boolean remove(int prev, int idx, int value, Lois lois);
	
	public abstract boolean insert(int idx, int value, Lois lois);

	public abstract boolean containsAny(int idx, int startValue, int endValue, Lois lois);
	
}
