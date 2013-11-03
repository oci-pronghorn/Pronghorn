package com.ociweb.jfast;



public class ValueDictionaryEntry {
	
	/*
	 * The previous value can be in one of three states: undefined, empty and assigned.

All values are in the state undefined when processing starts.
The assigned state indicates that the previous value is present and has some value.
The empty state is the assigned state but it indicates that previous value is absent. In such case previous values has "null" representaion of value.

	 */
	
	public final ValueDictionary  myDictionary;
	
	//can we used the byte interface instead and share it?
	public MyCharSequnce    charValue     = new MyCharSequnce();
	public byte[]          bytesValue;

	
	public DecimalDTO       decimalValue  = new DecimalDTO(); //promote interface to entry?
	public long            longValue;
	public int             intValue;	
	
	
	public boolean isNull; //TODO: once more operators are implemented may see pattern to replace this with function!
    public boolean isInit;
	
	//if this is an interface with a visit method?
	
	
	public ValueDictionaryEntry(ValueDictionary  myDictionary) {
		this.myDictionary = myDictionary;
	}
	

	public final void reset() {
		isInit = true;
		isNull = true;
		// TODO Auto-generated method stub
		
	}
		
}
