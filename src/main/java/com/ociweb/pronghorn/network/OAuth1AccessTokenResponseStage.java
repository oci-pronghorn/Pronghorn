package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class OAuth1AccessTokenResponseStage extends PronghornStage {

	public OAuth1AccessTokenResponseStage(GraphManager graphManager, 
			Pipe<NetResponseSchema> pipe,
			Pipe<ServerResponseSchema>[] outputPipes, 
			HTTPSpecification<?, ?, ?, ?> httpSpec) {
		super(graphManager, pipe, outputPipes);
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
