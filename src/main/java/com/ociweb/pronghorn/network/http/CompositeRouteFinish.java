package com.ociweb.pronghorn.network.http;

public interface CompositeRouteFinish {
	
	int routeId();
	int routeId(boolean debug);
	
	CompositeRouteFinish defaultInteger(String key, long value);	
	CompositeRouteFinish defaultText(String key, String value);
	CompositeRouteFinish defaultDecimal(String key, long m, byte e);	
	CompositeRouteFinish defaultRational(String key, long numerator, long denominator);
	CompositeRouteFinish associatedObject(String key, Object object);
}
