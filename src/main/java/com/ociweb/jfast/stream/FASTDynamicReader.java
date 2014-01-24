package com.ociweb.jfast.stream;


public class FASTDynamicReader implements FASTReader {

	final FASTReaderDispatch staticReader;
	
	//read groups field ids and build repeating lists of tokens.
	
	//only look up the most recent value read and return it to the caller.
	FASTDynamicReader(FASTReaderDispatch staticReader) {
		this.staticReader=staticReader;
		//staticReader.
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
	public int readBytes(int id) {
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
	public int readText(int id) {
		// TODO Auto-generated method stub
		return -1;
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
