package com.ociweb.pronghorn.network.module;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;

public interface RestResponder<T extends Enum<T> & HTTPContentType> {

	
	DataOutputBlobWriter<ServerResponseSchema> beginResponse(int status, T contentType, int length);
	
	
}
