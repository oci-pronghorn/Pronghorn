package com.ociweb.jfast;

//passed to and from clients for in-line of loop behavior.
public final class CharIterator implements CharSequence {

	char[] buffer;
	
	CharIterator(int maxSize) {
		buffer = new char[maxSize];
	}
	
	@Override
	public int length() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public char charAt(int index) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean hasNext() {
		return false;
	}
	
	public char next() {
		return 0;
	}
	
	public void dispose() {
		assert(true);//TODO: confirm release before use but only with assert on.
	}
}
