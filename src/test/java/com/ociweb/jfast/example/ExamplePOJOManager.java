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
