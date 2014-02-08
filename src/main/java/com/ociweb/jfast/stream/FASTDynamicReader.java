//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;


public class FASTDynamicReader {

	final FASTReaderDispatch readerDispatch;
	
	//read groups field ids and build repeating lists of tokens.
	
	//only look up the most recent value read and return it to the caller.
	FASTDynamicReader(FASTReaderDispatch dispatch) {
		this.readerDispatch = dispatch;
		
	}
	
	/**
	 * Read up to the end of the next sequence or message (eg. a repeating group)
	 * 
	 * Rules for making client compatible changes to templates.
	 * - Field can be demoted to more general common value before the group.
	 * - Field can be promotd to more specific value inside sequence
	 * - Field order inside group can change but can not cross sequence boundary.
	 * - Group boundaries can be added or removed.
	 * 
	 * Note nested sequence will stop once for each of the sequences therefore at the
	 * bottom hasMore may not have any new data but is only done as a notification that
	 * the loop has completed.
	 * 
	 * @return
	 */
	
	public int hasMore() {
		
		//readerDispatch.dispatchReadByToken(id, token);
		
		return -1;//return field id of the group just read
	}
	
	public int hasField() {
		
		return -1;//return field id of field just read.
	}

	public int readInt(int id) {
		return readerDispatch.lastInt(id);
	}

	public long readLong(int id) {
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
