//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.example;

public class ExamplePOJOManager {

	final ExamplePOJO[] testData;
	
	public ExamplePOJOManager(int sampleSize) {
		
		testData = new ExamplePOJO[sampleSize];
		int s = sampleSize;
		while (--s>=0) {
			testData[s] = new ExamplePOJO(s);
		}		
	}
		
	public ExamplePOJO[] testData() {
		return testData;
	}
	
	
}
