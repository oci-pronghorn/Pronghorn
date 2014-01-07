package com.ociweb.jfast.stream;

public class DynamicStream {

	private final FASTReaderCallback callback;
	private final FASTStaticReader staticReader;
	private final FASTDynamicReader dynamicReader;
	
	
	//TODO: this class functions as an iterator?
	//read message then call for the fields.?
	
	public DynamicStream(FASTReaderCallback callback, FASTStaticReader staticReader) {
		this.callback = callback;
		this.staticReader = staticReader;
		this.dynamicReader = new FASTDynamicReader(staticReader);
	}
	
	//read one full template
	//will be called repeatedly 
	public void readMessage() {
		
		//TODO: where are the arrays of tokens kept?
		
		//read each token in array
		//if token is group must use stack and call in
		//at end of group call callback.
		
		
	}
	
	
}
