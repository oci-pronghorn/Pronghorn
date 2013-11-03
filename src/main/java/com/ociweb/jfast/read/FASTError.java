package com.ociweb.jfast.read;

public enum FASTError {
	NO_OP_NO_PMAP("Field must not appear in pmap."), 
	CONST_INIT("Constant manditory fields must have an initial value."),
	MANDATORY_CONSTANT_NULL("Mandatory constant must not be null."),
	;

	private String text;
	FASTError(String text) {
		this.text = text;
	}
	
	public String toString() {
		return text;
	}
	
}
