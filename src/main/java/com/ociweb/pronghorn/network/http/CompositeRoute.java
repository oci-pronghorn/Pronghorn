package com.ociweb.pronghorn.network.http;

public interface CompositeRoute {
	
	int routeId();
	
	CompositeRoute defaultInteger(String key, long value);	
	CompositeRoute defaultText(String key, String value);
	CompositeRoute defaultDecimal(String key, long m, byte e);	
	CompositeRoute defaultRational(String key, long numerator, long denominator);
	
}
