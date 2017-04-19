package com.ociweb.pronghorn.network.mqtt;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.MQTTConnectionOutSchema;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MQTTClientResponseStage extends PronghornStage {

	public MQTTClientResponseStage(GraphManager gm, ClientCoordinator ccm, 
			Pipe<NetPayloadSchema>[] fromBroker,
			Pipe<ReleaseSchema> ackReleaseForResponseParser, 
			Pipe<MQTTConnectionOutSchema> out,
			Pipe<MQTTIdRangeSchema> idGenOut) {
		
		super(gm, fromBroker, join(ackReleaseForResponseParser, out, idGenOut));
		
	}

	@Override
	public void run() {
		
		// TODO Auto-generated method stub
		
	}

}
