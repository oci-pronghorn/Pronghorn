package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public interface ClientResponseParserFactory {

	void buildParser(GraphManager gm, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] clearResponse,
			Pipe<ReleaseSchema> ackReleaseForResponseParser);

}
