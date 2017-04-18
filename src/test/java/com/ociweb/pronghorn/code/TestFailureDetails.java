package com.ociweb.pronghorn.code;

public class TestFailureDetails {

	private final String note;
	
	public TestFailureDetails(String note) {
		this.note = note;
	}
	
	public String toString() {
		return note;
	}
	
}
