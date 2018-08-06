package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.struct.ByteSequenceValidator;
import com.ociweb.pronghorn.struct.DecimalValidator;
import com.ociweb.pronghorn.struct.LongValidator;

public interface CompositeRouteFinish {
	
	int routeId();
	
	/**
	 * Register this object for use in looking up either the 
	 * routeId or the structId
	 * 
	 * @param associatedObject
	 * @return routeId
	 */
	int routeId(Object associatedObject);
	
	CompositeRouteFinish defaultInteger(String key, long value);	
	CompositeRouteFinish defaultText(String key, String value);
	CompositeRouteFinish defaultDecimal(String key, long m, byte e);	
	CompositeRouteFinish defaultRational(String key, long numerator, long denominator); //for %{name}  ?? 1/2  in the future
	CompositeRouteFinish associatedObject(String key, Object associatedObject);
	CompositeRouteFinish validator(String key, LongValidator validator);
	CompositeRouteFinish validator(String key, ByteSequenceValidator validator);	
	CompositeRouteFinish validator(String key, DecimalValidator validator);	
	
	CompositeRouteFinish refineInteger(String key, Object associatedObject, long defaultValue);
	CompositeRouteFinish refineText(   String key, Object associatedObject, String defaultValue);
	CompositeRouteFinish refineDecimal(String key, Object associatedObject, long defaultMantissa, byte defaultExponent);
	
	CompositeRouteFinish refineInteger(String key, Object associatedObject, long defaultValue, LongValidator validator);
	CompositeRouteFinish refineText(   String key, Object associatedObject, String defaultValue, ByteSequenceValidator validator);
	CompositeRouteFinish refineDecimal(String key, Object associatedObject, long defaultMantissa, byte defaultExponent, DecimalValidator validator);
		
	CompositeRouteFinish refineInteger(String key, Object associatedObject, LongValidator validator);
	CompositeRouteFinish refineText(   String key, Object associatedObject, ByteSequenceValidator validator);
	CompositeRouteFinish refineDecimal(String key, Object associatedObject, DecimalValidator validator);
	
}
