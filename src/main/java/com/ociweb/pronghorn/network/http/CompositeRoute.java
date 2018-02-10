package com.ociweb.pronghorn.network.http;

public class CompositeRoute {

	protected CompositeRoute() {
		
	}
	
	public int routeId() {
		//last command returns the id;
		
		return -1;
	}

	public CompositeRoute path(String path) {
		
		
		//NOTE: if path does not start with / then we add it with no complaint.
		
		return this;
	}
	
	
	public CompositeRoute defaultInteger(String key, long value) {
		
		return this;
	}
	
	public CompositeRoute defaultText(String key, String value) {
		
		return this;
	}
	
	public CompositeRoute defaultDecimal(String key, long m, byte e) {
		
		return this;
	}
	
	public CompositeRoute defaultRational(String key, long numerator, long denominator) {
		
		return this;
	}
	
}
