package com.ociweb.pronghorn.network.module;

import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;

public interface SimpleRestLogic {

	void process(DataInputBlobReader<HTTPRequestSchema> inputStream, RestResponder responder);

}
