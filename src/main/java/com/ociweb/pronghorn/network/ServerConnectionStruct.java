package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructTypes;

public class ServerConnectionStruct {

	//extract to object to hand off to the server coordinator
	public final StructRegistry registry;	
	public static enum connectionFields {
		arrivalTime,
		context;
	}	
	public final int connectionStructId;
	
	public final long contextFieldId;
	public final long arrivalTimeFieldId;

	public ServerConnectionStruct(StructRegistry recordTypeData) {
		this.registry = recordTypeData;
		
		byte[][] fieldNames = new byte[][]{
							"arrival".getBytes(),
			                "context".getBytes()
			              };
		StructTypes[] structTypes = new StructTypes[] {
				StructTypes.Long, StructTypes.Integer
				                   };
		int [] fieldDims = new int[]{0,0};
		Object[] fieldAssoc = new Object[] {
				connectionFields.arrivalTime, connectionFields.context
				    };
		
		//keeps the requestContext for use upon response.
		this.connectionStructId = recordTypeData.addStruct(
				fieldNames,
				structTypes,
				fieldDims,
				fieldAssoc				
				);
		
        contextFieldId = registry.fieldLookupByIdentity(
                connectionFields.context, connectionStructId);
        
        arrivalTimeFieldId = registry.fieldLookupByIdentity(
                connectionFields.arrivalTime, connectionStructId);
				
        System.err.println("total size of indexes "+registry.totalSizeOfIndexes(connectionStructId));
        
        
	}	
	
}
