package com.ociweb.jfast.stream;

public class DynamicStream {

	private final FASTReaderCallback callback;
	private final FASTReaderDispatch dispatch;
	private final FASTDynamicReader dynamicReader;
	
	
	//TODO: this class functions as an iterator?
	//read message then call for the fields.?
	
	public DynamicStream(FASTReaderCallback callback, FASTReaderDispatch dispatch) {
		this.callback = callback;
		this.dispatch = dispatch;
		this.dynamicReader = new FASTDynamicReader(dispatch);
	}
	
	//read one full template
	//will be called repeatedly 
	public void readMessage() {
		
		//TODO: where are the arrays of tokens kept?
		
		//read each token in array
		//if token is group must use stack and call in
		//at end of group call callback.
		
		
	}
	
	//thoughts
	//can use different function for next value
	//can quit any time
	//must support random field access order.
	//must support calling the pre-compiled template dispatch.
	//when the template is parsed and the binary file produced the java can also be produced.
	
}
