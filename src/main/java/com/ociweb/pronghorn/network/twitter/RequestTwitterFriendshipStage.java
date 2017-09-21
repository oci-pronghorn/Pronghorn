package com.ociweb.pronghorn.network.twitter;

import com.ociweb.pronghorn.network.OAuth1HeaderBuilder;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.http.HTTPClientUtil;
import com.ociweb.pronghorn.network.http.HTTPUtil;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RequestTwitterFriendshipStage extends PronghornStage {

	private final String ck;
	private final String cs;
	private final String token;
	private final String secret;
	private OAuth1HeaderBuilder myAuth;
	
	private static final int port = 443;
	private static final String domain = "api.twitter.com";
	
	private static final String followPath   = "/1.1/friendships/create.json";
	private static final String unfollowPath = "/1.1/friendships/destroy.json";
	
	private final byte[] path;
	private final byte[][] postItems;
	private final int msBetweenCalls;
	
	private final Pipe<HTTPRequestSchema>[] inputs;
	private final Pipe<ServerResponseSchema>[] outputs;	
	private final Pipe<ClientHTTPRequestSchema> toTwitter;
	private long nextTriggerTime = -1;
	
	/////////////
	//POST https://api.twitter.com/1.1/friendships/create.json?user_id=1401881&follow=true
    //POST https://api.twitter.com/1.1/friendships/destroy.json?user_id=1401881
	/////////////		
	
	public static RequestTwitterFriendshipStage newFollowInstance(GraphManager graphManager,
			String ck, String cs, 
			String token, String secret, 
			Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs,
			Pipe<ClientHTTPRequestSchema> toTwitter) {
		return new RequestTwitterFriendshipStage(graphManager, ck, cs, token, secret, 
				inputs, outputs, toTwitter, followPath.getBytes(), "follow=true".getBytes());			 
	}
	
	public static RequestTwitterFriendshipStage newUnFollowInstance(GraphManager graphManager,
			String ck, String cs, 
			String token, String secret, 
			Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs,
			Pipe<ClientHTTPRequestSchema> toTwitter) {
		return new RequestTwitterFriendshipStage(graphManager, ck, cs, token, secret, 
				inputs, outputs, toTwitter, unfollowPath.getBytes());			 
	}
	
	private RequestTwitterFriendshipStage(
			GraphManager graphManager,
			String ck, String cs, 
			String token, String secret, 
			Pipe<HTTPRequestSchema>[] inputs, 
			Pipe<ServerResponseSchema>[] outputs,
			Pipe<ClientHTTPRequestSchema> toTwitter, //http request going out
			byte[] path,
			byte[] ... postItems
			) {
		
		super(graphManager, inputs, join(outputs, toTwitter));
		
		this.inputs = inputs;
		this.outputs = outputs;
		this.toTwitter = toTwitter;
		
		this.ck = ck;
	    this.cs = cs;
		this.token = token;
		this.secret = secret;
			
		this.path = path;
		this.postItems = postItems;
		
		this.msBetweenCalls = (24*60*60*1000)/1000;  //(15*60*1000)/15; 15 min limit
		assert(86_400 == msBetweenCalls);  //24 hour limit of 1000
				
	}
	
	@Override
	public void startup() {
		myAuth = new OAuth1HeaderBuilder(ck, cs, token, secret);		
	}
	
	@Override
	public void run() {
		
		int i = inputs.length;
		while (--i>=0) {
			run(inputs[i],outputs[i],toTwitter);
		}

	}

	private void run(Pipe<HTTPRequestSchema> input,
			         Pipe<ServerResponseSchema> output,
			         Pipe<ClientHTTPRequestSchema> toTwitter) {
				
		while (Pipe.hasContentToRead(input) 
				&& Pipe.hasRoomForWrite(output)
				&& Pipe.hasRoomForWrite(toTwitter)) {
			
			if (isThisTheTime()) {
				//do it now, toTwitter then take response and relay it back
				
				//TODO: write twitter call..
				
				
			} else {
				//too quickly, send back 420
				long connectionId = -1;
				int sequenceID = -1;
								
				int contentLength = 0;
				byte[] contentBacking = null;
				int contentPosition = 0;
				int contentMask = Integer.MAX_VALUE;
				
				
				
				int channelIdHigh = (int)(connectionId>>32); 
				int channelIdLow = (int)connectionId;
				
				HTTPUtil.simplePublish(ServerCoordinator.END_RESPONSE_MASK | ServerCoordinator.CLOSE_CONNECTION_MASK, 
						      sequenceID, 420, output, channelIdHigh, channelIdLow, null,
						      contentLength, contentBacking, contentPosition, contentMask);
	
			}
			
			
			
			
		}
	}

	private boolean isThisTheTime() {
		long now = System.currentTimeMillis();		
		if (now > nextTriggerTime) {
			nextTriggerTime = now + msBetweenCalls;
			return true;
		} else {
			return false;
		}

	}

}
