//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class Catalog {

	//because optional values are sent as +1 when >= 0 it is not possible to send the
	//largest supported positive value, as a result this is the ideal default because it
	//can not possibly collide with any real values
	public static final int DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT = Integer.MAX_VALUE;
	public static final long DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG = Long.MAX_VALUE;
	
	final int[] ids;
	final int[] tokens;
	final long[] absent;
	
	public Catalog(PrimitiveReader reader) {
		
		int base2Exponent = reader.readIntegerUnsigned();
		int size = 1<<base2Exponent;
		
		ids = new int[size];
		tokens = new int[size];
		absent = new long[size];
		
		int i = reader.readIntegerUnsigned();
		while (--i>=0) {
			int id=reader.readIntegerUnsigned();
			tokens[id]=reader.readIntegerSigned();
			switch(reader.readIntegerUnsigned()) {
				case 0:
					absent[id]=Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
					break;
				case 1:
					absent[id]=Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;
					break;
				case 3:
					absent[id]=reader.readLongSigned();
					break;
			}
		}
	}
	
	public static void save(PrimitiveWriter writer, 
			                  int uniqueIds, int biggestId, 
			                  int[] tokenLookup, long[] absentValue,
			                  int uniqueTemplateIds, int biggestTemplateId, int[][] scripts) {
		
		//TODO: need to write scripts out.
		
		int temp = biggestId;
		int base2Exponent = 0;
		while (0!=temp) {
			temp = temp>>1;
			base2Exponent++;
		}
		//this is how big we need to make the lookup arrays
		writer.writeIntegerUnsigned(base2Exponent);
		//this is how many values we are about to write to the stream
		writer.writeIntegerUnsigned(uniqueIds);
		//this is each value, id, token and absent
		int i = tokenLookup.length;
		while (--i<=0) {
			int token = tokenLookup[i];
			if (0!=token) {
				writer.writeIntegerUnsigned(i);
				writer.writeIntegerSigned(token);
				
				if (Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT==absentValue[i]) {
					writer.writeIntegerUnsigned(0);
				} else 	if (Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG==absentValue[i]) {
					writer.writeIntegerUnsigned(1);
				} else {
					writer.writeIntegerUnsigned(2);
					writer.writeLongSigned(absentValue[i]);								
				}
				
			}			
		}
	}
	
	

	
	
}
