package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class OAuth1RequestTokenResponseStage extends PronghornStage {

	private Pipe<NetResponseSchema> input;
	private Pipe<ServerResponseSchema>[] output;
	
	public OAuth1RequestTokenResponseStage(GraphManager graphManager, 
			Pipe<NetResponseSchema> input,
			Pipe<ServerResponseSchema>[] output) {
		super(graphManager, input, output);
	}

	@Override
	public void run() {
		
		while (Pipe.hasContentToRead(input)) {
		
			int msgIdx = Pipe.takeMsgIdx(input);
			
			if (NetResponseSchema.MSG_RESPONSE_101 == msgIdx) {
				
				long fieldConnectionId = Pipe.takeLong(input);
				int fieldContextFlags = Pipe.takeInt(input);
				DataInputBlobReader<NetResponseSchema> stream = Pipe.openInputStream(input);
				
				//Read examples from Twitter response Parser?
				
				//TODO: read the headers and get the request token
				//get  oauth_token
				//get  oauth_token_secret
				//get  oauth_callback_confirmed
				
				//TODO: send redirect server response
				
				
			} else if (NetResponseSchema.MSG_CLOSED_10 == msgIdx) {
			
				DataInputBlobReader<NetResponseSchema> host = Pipe.openInputStream(input);
				int port = Pipe.takeInt(input);
				
			} else {
				//unexpected continuation??
				
				long fieldConnectionId = Pipe.takeLong(input);
				int fieldContextFlags = Pipe.takeInt(input);
				DataInputBlobReader<NetResponseSchema> stream = Pipe.openInputStream(input);
				
				
			}
				
			
			
			
		}
		
	}

}
