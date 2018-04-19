package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructTypes;

public class ServerConnectionStruct {

	//extract to object to hand off to the server coordinator
	public final StructRegistry registry;	
	public static enum connectionFields {
		context;
	}	
	public final int connectionStructId;

	public ServerConnectionStruct(StructRegistry recordTypeData) {
		this.registry = recordTypeData;
		
		byte[][] fieldNames = new byte[][]{"context".getBytes()};
		StructTypes[] structTypes = new StructTypes[] {StructTypes.Integer};
		int [] fieldDims = new int[]{0};
		Object[] fieldAssoc = new Object[] {connectionFields.context};
		
		//keeps the requestContext for use upon response.
		this.connectionStructId = recordTypeData.addStruct(
				fieldNames,
				structTypes,
				fieldDims,
				fieldAssoc				
				);
				
	}
	
	
}
