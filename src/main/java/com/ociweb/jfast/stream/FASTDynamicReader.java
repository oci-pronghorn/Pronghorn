package com.ociweb.jfast.stream;


public class FASTDynamicReader {

	final FASTReaderDispatch readerDispatch;
	
	//read groups field ids and build repeating lists of tokens.
	
	//only look up the most recent value read and return it to the caller.
	FASTDynamicReader(FASTReaderDispatch dispatch) {
		this.readerDispatch = dispatch;
		
	}
	
	public int hasMore() {
		
		//readerDispatch.dispatchReadByToken(id, token);
		
		return -1;
	}
	
	public int hasField() {
		
		return -1;
	}

	public int readInt(int id, int valueOfOptional) {
		return readerDispatch.lastInt(id);
	}

	public long readLong(int id, long valueOfOptional) {
		return readerDispatch.lastLong(id);
	}

	public int readBytes(int id) {
		return readerDispatch.lastInt(id);
	}

	public int readDecimalExponent(int id) {
		return readerDispatch.lastInt(id);
	}

	public long readDecimalMantissa(int id) {
		return readerDispatch.lastLong(id);
	}
	
	public int readText(int id) {
		return readerDispatch.lastInt(id);
	}

	public void openGroup(int maxPMapBytes) {
		// TODO Auto-generated method stub
		
	}

	public void closeGroup(int id) {
		// TODO Auto-generated method stub
		
	}


}
