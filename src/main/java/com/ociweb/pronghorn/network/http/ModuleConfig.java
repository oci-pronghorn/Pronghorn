package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public interface ModuleConfig {

	int moduleCount(); 

	Pipe<ServerResponseSchema>[] registerModule(int moduleInstance, 
			                                    GraphManager graphManager,
			                                    RouterStageConfig routerConfig,
			                                    Pipe<HTTPRequestSchema>[] inputPipes);

}
