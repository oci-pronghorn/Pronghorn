package com.ociweb.jfast.example.dynamic;

import com.ociweb.jfast.example.ExamplePOJO;
import com.ociweb.jfast.example.ExamplePOJOManager;

public class ExampleApp {

	
	
	public static void main(String[] args) {
	
		int testSize = 100000;
		ExamplePOJOManager dataManager = new ExamplePOJOManager(testSize);
		ExamplePOJO[] testData = dataManager.testData();
		
		//write data example
		
		//TODO: not ready yet
		
		//dynamic read data example
		
		//TODO: not ready yet
		
	}
	
	
}
