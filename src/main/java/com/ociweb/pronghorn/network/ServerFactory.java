package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public interface ServerFactory {

	void buildServer(GraphManager graphManager, ServerCoordinator coordinator, 
			         Pipe<ReleaseSchema>[] releaseAfterParse,
			         Pipe<NetPayloadSchema>[] receivedFromNet, 
			         Pipe<NetPayloadSchema>[] sendingToNet);

}
