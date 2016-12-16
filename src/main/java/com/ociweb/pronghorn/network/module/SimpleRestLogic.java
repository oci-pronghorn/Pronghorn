package com.ociweb.pronghorn.network.module;

import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;

public interface SimpleRestLogic {

	void process(DataInputBlobReader<HTTPRequestSchema> inputStream,
		     	 DataOutputBlobWriter<ServerResponseSchema> outputStream);

}
