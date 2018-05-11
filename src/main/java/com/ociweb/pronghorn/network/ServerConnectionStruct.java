package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructType;

public class ServerConnectionStruct {

	//extract to object to hand off to the server coordinator
	public final StructRegistry registry;
	
	public static enum connectionFields {
		arrivalTime,
		businessStartTime,
		routeId,
		context;
	}	
	private HTTPHeader[] headersToEcho;
	
	public final int connectionStructId;
	
	public final long contextFieldId;
	public final long arrivalTimeFieldId;
	public final long businessStartTime;
	public final long routeIdFieldId;

	private int minInternalInFlightCount = 1<<10;//must not be zero //TODO: add update method
	private int minInternalInFlightPayloadSize = 56;//TODO: add update method
		
	public int inFlightCount() {
		return minInternalInFlightCount;		
	}
	
	public int inFlightPayloadSize() {
		return minInternalInFlightPayloadSize;
	}
	
	public ServerConnectionStruct(StructRegistry recordTypeData) {
		this.registry = recordTypeData;
	
		int fieldsCount = 4;		
		byte[][] fieldNames = new byte[fieldsCount][];
		StructType[] structTypes = new StructType[fieldsCount];
		int [] fieldDims = new int[fieldsCount];//all zeros, no dim supported
		Object[] fieldAssoc = new Object[fieldsCount];
				
		fieldNames[0] = "arrival".getBytes();
		structTypes[0] = StructType.Long;
		fieldAssoc[0] = connectionFields.arrivalTime;
		
		fieldNames[1] = "business".getBytes();
		structTypes[1] = StructType.Long;
		fieldAssoc[1] = connectionFields.businessStartTime;
				
		fieldNames[2] = "context".getBytes();
		structTypes[2] = StructType.Integer;
		fieldAssoc[2] = connectionFields.context;
				
		fieldNames[3] = "routeId".getBytes();
		structTypes[3] = StructType.Integer;
		fieldAssoc[3] = connectionFields.routeId;

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
		
        routeIdFieldId = registry.fieldLookupByIdentity(
                connectionFields.routeId, connectionStructId);
        
	}
	
	public void maxInternalServerRequestsInFlight(int max) {
		minInternalInFlightCount = Math.max(max, minInternalInFlightCount);
	}

	public void headersToEcho(int maxSingleHeaderSize, HTTPHeader ... headers) {
		minInternalInFlightPayloadSize += (headers.length*maxSingleHeaderSize);
		headersToEcho = headers;
		
		for(int h = 0; h<headers.length; h++) {
			long id = registry.growStruct(connectionStructId, StructType.Blob, 0, headers[h].rootBytes());
			registry.setAssociatedObject(id, headers[h]);
		}
	}
	
	public HTTPHeader[] headersToEcho() {
		return headersToEcho;
	}

	public boolean isEchoHeader(HTTPHeader header) {
		if (null!=headersToEcho) {
			//NOTE: may be a better way to do this if we have a long list...
			int h = headersToEcho.length;
			while (--h>=0) {
				if (header == headersToEcho.clone()[h]) {
					return true;
				}
			}
		}
		return false;
	}	
	
}
