package com.ociweb.pronghorn.util.primitive;

public class LoisOpRLE extends LoisOperator {

	private final int id;
		
	public LoisOpRLE(int id) {
		this.id = id;
	}

	@Override
	public boolean isBefore(int idx, int value, Lois lois) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isAfter(int idx, int value, Lois lois) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean remove(int prev, int idx, int value, Lois lois) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean insert(int idx, int value, Lois lois) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean visit(int idx, LoisVisitor visitor, Lois lois) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsAny(int idx, int startValue, int endValue, Lois lois) {
		// TODO Auto-generated method stub
		return false;
	}

}
