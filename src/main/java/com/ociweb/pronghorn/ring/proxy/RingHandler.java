package com.ociweb.pronghorn.ring.proxy;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RingHandler {

	private static final Logger log = LoggerFactory.getLogger(RingHandler.class);
	
	protected final int MAX_METHODS = 1024;//based on how the key hash is built, do not change
	
	
	private final int c1;
	private final int c2;
	private final int c3;
	
	protected RingHandler(final Method[] methods) {
		//Compute the shortest name and
		//computed the leading chars that all match
		
		int minMethodNameLength = Integer.MAX_VALUE;
		int leadingMatchingChars = Integer.MAX_VALUE;
		
		
		
		int j = methods.length;
		String lastName = null;
		while (--j>=0) {
			Method method = methods[j];			
			ProngTemplateField fieldAnnonation = method.getAnnotation(ProngTemplateField.class);
			if (null!=fieldAnnonation) {
				String methodName = method.getName();
				minMethodNameLength = Math.min(minMethodNameLength, methodName.length());
				if (null!=lastName) {
					int limit = Math.min(methodName.length(), lastName.length());
					int i = 0;
					while (i<limit && lastName.charAt(i)==methodName.charAt(i)) {
						i++;
					}
					leadingMatchingChars = Math.min(leadingMatchingChars, i);					
				}
				lastName = methodName;
			}
		}
				
		
		int posToCheck = minMethodNameLength-leadingMatchingChars;
		
		if (posToCheck <= 0) {
			throw new UnsupportedOperationException("Method names are too similar. Every annotated field must have a different field name.");
		}
		
		int cursor = 0; //start out with nothing set
		
		//this is the default position which is better than zero
		int tempC1 = leadingMatchingChars+1;
		int tempC2 = leadingMatchingChars+1; 
		int tempC3 = leadingMatchingChars+1; 
		
		
		//TODO: need to confirm that values are less than minMethodNameLength and together lead to a unique value.
		
		//find those with the same name length
		//of those check that c1 makes them unique
		//once c1 is set must move to c2 to make the next unique.
		//loop here and make nested loop call?
		int m = methods.length;
		while (--m>=0) {
			Method method1 = methods[m];			
			if (null!=method1.getAnnotation(ProngTemplateField.class)) {
				String methodName = method1.getName();
				//System.err.println(methodName);
				
				//scan all the method names down from this one to check for another with the same length
				int k = m;
				while (--k>=0) {
					Method method2 = methods[k];			
					if (null!=method2.getAnnotation(ProngTemplateField.class)) {
						String methodName2 = method2.getName();		
						//System.err.println("   "+methodName2);
						
						if (namesBuildSameKey(methodName2, methodName, tempC1, tempC2, tempC3, cursor)) {
							//throws if nothing can be done	
							switch (cursor) {
								case 0:
									tempC1 = addColumnToDistinquishNames(methodName2, methodName, cursor, leadingMatchingChars, minMethodNameLength);	
									break;
								case 1:
									tempC2 = addColumnToDistinquishNames(methodName2, methodName, cursor, leadingMatchingChars, minMethodNameLength);	
									break;
								case 2:
									tempC3 = addColumnToDistinquishNames(methodName2, methodName, cursor, leadingMatchingChars, minMethodNameLength);	
									break;
							    default:
							    	throw new UnsupportedOperationException("Method names are too similar. Every annotated field must have a different field name.");
							}
							cursor++;
						}
					}
				}
			}
		}	
		
		this.c1 = tempC1;
		this.c2 = tempC2;
		this.c3 = tempC3;
		
		log.trace("All methods share the first {} chars in common", leadingMatchingChars);
		log.trace("Shortest method names is {} chars", minMethodNameLength);
		log.trace("With length these additional char positions make name key unique {}, {}, {}",tempC1,tempC2,tempC3);
	}
	
	private int addColumnToDistinquishNames(String methodName2, String methodName, int cursor, int leadingMatchingChars, int minMethodNameLength) {
		
		if (cursor>=3 || methodName.equals(methodName2)) {
			//can't add any more columns, perhaps we should use 4 of these per int or look at using a long for the value.
			throw new UnsupportedOperationException("Method names are too similar. Every annotated field must have a different field name.");
		}
		
		int i = minMethodNameLength;
		while (--i > leadingMatchingChars) {
			if ((3&methodName2.charAt(i)) != (3&methodName.charAt(i))) { //these masks must match buildKey
				return i;
			}
		}
		throw new UnsupportedOperationException("Method names are too similar. Every annotated field must have a different field name.");
	}

	//TODO: change to map into small space to remove the hash table6
	
	private static boolean namesBuildSameKey(String methodName2, String methodName, int c1, int c2, int c3, int cursor) {
		return buildKey(methodName2, c1, c2, c3, cursor) == buildKey(methodName, c1, c2, c3, cursor);
	}
	
	
	//NOTE: if we need more than 128 methods can add c4 etc.
	protected static int buildKey(RingHandler handler, String value) {
		//builds a value 7 bits long and no longer
		return ((int)(15&value.length())<<6) | ((int)(3&value.charAt(handler.c1))<<4) | ((int)(3&value.charAt(handler.c2))<<2) | ((int)(3&value.charAt(handler.c3)));
				
	}
	
	private static int buildKey(String value, int c1, int c2, int c3, int cursor) {
		switch (cursor) {
			case 0:
				return ((int)(15&value.length())<<6);
			case 1:
				return ((int)(15&value.length())<<6) | ((int)(3&value.charAt(c1))<<4);
			case 2:
				return ((int)(15&value.length())<<6) | ((int)(3&value.charAt(c1))<<4) | ((int)(3&value.charAt(c2))<<2);
			default:
				return ((int)(15&value.length())<<6) | ((int)(3&value.charAt(c1))<<4) | ((int)(3&value.charAt(c2))<<2) | ((int)(3&value.charAt(c3)));
		}		
	}

}
