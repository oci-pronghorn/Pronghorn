package com.ociweb.pronghorn.network.mqtt;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.ClientResponseParserFactory;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.schema.MQTTConnectionInSchema;
import com.ociweb.pronghorn.network.schema.MQTTConnectionOutSchema;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MQTTClientGraphBuilder {

	
	public void buildMQTTClientGraph(GraphManager gm) {
		
		final boolean isTLS = true;
		final int connectionsInBits = 2;
		final int maxPartialResponses = 4;
		
		Pipe<MQTTConnectionInSchema> in = MQTTConnectionInSchema.instance.newPipe(20, 2048); //from the application 
		Pipe<MQTTConnectionOutSchema> out = MQTTConnectionOutSchema.instance.newPipe(20, 2048); //from the response
		Pipe<MQTTIdRangeSchema> idGenOut = MQTTConnectionOutSchema.instance.newPipe(10,0);
		
		int ttlSec = 10;//seconds.
		new MQTTAPIStage(gm, idGenOut, out, in, ttlSec); //TODO: need to rethink.
		
		
		ClientCoordinator ccm = new ClientCoordinator(connectionsInBits, maxPartialResponses);
		
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {
			
			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] fromBroker,
									Pipe<ReleaseSchema> ackReleaseForResponseParser) {
				
				//parse the socket data, determine the message and write it to the out pipe
				new MQTTClientResponseStage(gm, ccm, fromBroker, ackReleaseForResponseParser, out, idGenOut); //releases Ids.
		
			}
		};
		
		int minimumFragmentsOnRing = 4;
		int maximumLenghOfVariableLengthFields = 4096;
		int outgoingPipes = 1;
		Pipe<NetPayloadSchema>[] toBroker = Pipe.buildPipes(outgoingPipes, NetPayloadSchema.instance.newPipeConfig(minimumFragmentsOnRing, maximumLenghOfVariableLengthFields));
		
		//take input request and write the bytes to the broker socket
		new MQTTClientRequestStage(gm, in, toBroker);

		NetGraphBuilder.buildSimpleClientGraph(gm, isTLS, ccm, factory, toBroker);
		
		
		
		
	}
	
}
