package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

public class FASTDynamicReader  implements FASTReader {

	final FASTStaticReader staticReader;
	
	//only look up the most recent value read and return it to the caller.
	FASTDynamicReader(FASTStaticReader staticReader) {
		this.staticReader=staticReader;
	}

	@Override
	public int readInt(int id, int valueOfOptional) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long readLong(int id, long valueOfOptional) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void readBytes(int id, ByteBuffer target) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int readBytes(int id, byte[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int readDecimalExponent(int id, int valueOfOptional) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public long readDecimalMantissa(int id, long valueOfOptional) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public void readChars(int id, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int readChars(int id, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void openGroup(int maxPMapBytes) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void closeGroup(int id) {
		// TODO Auto-generated method stub
		
	}


}
