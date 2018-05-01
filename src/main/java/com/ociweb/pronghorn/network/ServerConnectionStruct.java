package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructTypes;

public class ServerConnectionStruct {

	//extract to object to hand off to the server coordinator
	public final StructRegistry registry;
	
	public static enum connectionFields {
		arrivalTime,
		businessStartTime,
		context;
	}	
	private HTTPHeader[] headersToEcho;
	
	public final int connectionStructId;
	
	public final long contextFieldId;
	public final long arrivalTimeFieldId;
	public final long businessStartTime;

	private int minInternalInFlightCount = 1<<10;//must not be zero //TODO: add update method
	private int minInternalInFlightPayloadSize = 48;//TODO: add update method
	
	
	public int inFlightCount() {
		return minInternalInFlightCount;		
	}
	
	public int inFlightPayloadSize() {
		
		//TODO: the payload will requrie this header space as well for clients.. must integrate..
		
		
		return minInternalInFlightPayloadSize;
	}
	
	public ServerConnectionStruct(StructRegistry recordTypeData) {
		this.registry = recordTypeData;
		
		int fieldsCount = 3 + (null == headersToEcho ? 0 : headersToEcho.length);
		
		byte[][] fieldNames = new byte[fieldsCount][];
		StructTypes[] structTypes = new StructTypes[fieldsCount];
		int [] fieldDims = new int[fieldsCount];//all zeros, no dim supported
		Object[] fieldAssoc = new Object[fieldsCount];
				
		fieldNames[0] = "arrival".getBytes();
		structTypes[0] = StructTypes.Long;
		fieldAssoc[0] = connectionFields.arrivalTime;
		
		fieldNames[1] = "business".getBytes();
		structTypes[1] = StructTypes.Long;
		fieldAssoc[1] = connectionFields.businessStartTime;
				
		fieldNames[2] = "context".getBytes();
		structTypes[2] = StructTypes.Integer;
		fieldAssoc[2] = connectionFields.context;
				
		int e = 0;
		for(int r = 3; r<fieldsCount; r++) {
			HTTPHeader header = headersToEcho[e++];
			fieldNames[r] = header.rootBytes();
			structTypes[r] = StructTypes.Blob;
			fieldAssoc[r] = header;
		}
	
		//keeps the requestContext, header echos and arrival time for use upon response.
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
				
        businessStartTime = registry.fieldLookupByIdentity(
                connectionFields.businessStartTime, connectionStructId);
		
	}
	
	public void maxInternalServerRequestsInFlight(int max) {
		minInternalInFlightCount = Math.max(max, minInternalInFlightCount);
	}

	public void headersToEcho(int maxSingleHeaderSize, HTTPHeader ... headers) {
		minInternalInFlightPayloadSize += (headers.length*maxSingleHeaderSize);
		headersToEcho = headers;
	}
	
	public HTTPHeader[] headersToEcho() {
		return headersToEcho;
	}	
	
}
