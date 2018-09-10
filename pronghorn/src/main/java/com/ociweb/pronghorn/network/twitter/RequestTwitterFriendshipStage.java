package com.ociweb.pronghorn.network.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.OAuth1HeaderBuilder;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.http.HTTPUtil;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

/**
 * _no-docs_
 */
public class RequestTwitterFriendshipStage extends PronghornStage {

	//TODO: should be easy to make send tweet stage after this.
	private static final Logger logger = LoggerFactory.getLogger(RequestTwitterFriendshipStage.class);
	
	private final String ck;
	private final String cs;
	private final String token;
	private final String secret;
	private OAuth1HeaderBuilder myAuth;
	
	private static final int port = 443;
	private static final String host = "api.twitter.com";
	
	
	private static final String followPath   = "/1.1/friendships/create.json";
	private static final String unfollowPath = "/1.1/friendships/destroy.json";
	
	private final String path;
	private final byte[] pathBytes;
	private final byte[][] postItems;
	private final int msBetweenCalls;
	
	private final Pipe<HTTPRequestSchema> inputs;
	private final Pipe<ServerResponseSchema> outputs;	
	private final Pipe<ClientHTTPRequestSchema> toTwitter;
	private final Pipe<NetResponseSchema> fromTwitter;
	
	
	private final String prefix = "user_id=";
	private final String postfix = "&follow=true";
	
	private long nextTriggerTime = -1;
	
	private final int contentPosition = 0;
	private final int contentMask = Integer.MAX_VALUE;
	private byte[] contentBacking;
	
	private final StringBuilder dynamicLength;
	private final int httpRequestResponseId;
	
	/////////////
	//POST https://api.twitter.com/1.1/friendships/create.json?user_id=1401881&follow=true
    //POST https://api.twitter.com/1.1/friendships/destroy.json?user_id=1401881
	/////////////		
	
	public static RequestTwitterFriendshipStage newFollowInstance(GraphManager graphManager,
			String ck, String cs, 
			String token, String secret, 
			Pipe<HTTPRequestSchema> inputs, Pipe<ServerResponseSchema> outputs,
			int httpRequestResponseId,
			Pipe<ClientHTTPRequestSchema> toTwitter, Pipe<NetResponseSchema> fromTwitter) {
		return new RequestTwitterFriendshipStage(graphManager, ck, cs, token, secret, 
				inputs, outputs, httpRequestResponseId, toTwitter, fromTwitter, followPath, "follow=true".getBytes());			 
	}
	
	public static RequestTwitterFriendshipStage newUnFollowInstance(GraphManager graphManager,
			String ck, String cs, 
			String token, String secret, 
			Pipe<HTTPRequestSchema> inputs, Pipe<ServerResponseSchema> outputs,
			int httpRequestResponseId,
			Pipe<ClientHTTPRequestSchema> toTwitter, Pipe<NetResponseSchema> fromTwitter) {
		return new RequestTwitterFriendshipStage(graphManager, ck, cs, token, secret, 
				inputs, outputs, httpRequestResponseId, toTwitter, fromTwitter, unfollowPath);			 
	}
	
	private RequestTwitterFriendshipStage(
			GraphManager graphManager,
			String ck, String cs, 
			String token, String secret, 
			Pipe<HTTPRequestSchema> inputs, 
			Pipe<ServerResponseSchema> outputs,
			int httpRequestResponseId,
			Pipe<ClientHTTPRequestSchema> toTwitter, 
			Pipe<NetResponseSchema> fromTwitter,
			String path,
			byte[] ... postItems
			) {
		
		super(graphManager, join(inputs, fromTwitter), join(outputs, toTwitter));
		
		this.inputs = inputs;
		this.outputs = outputs;
		this.toTwitter = toTwitter;
		this.fromTwitter = fromTwitter;
		
		this.ck = ck;
	    this.cs = cs;
		this.token = token;
		this.secret = secret;
			
		this.path = path;
		this.pathBytes = path.getBytes();
		this.postItems = postItems;	
		this.httpRequestResponseId = httpRequestResponseId;
		
		this.msBetweenCalls = (24*60*60*1000)/1000;  //(15*60*1000)/15; 15 min limit
		assert(86_400 == msBetweenCalls);  //24 hour limit of 1000
		this.dynamicLength = new StringBuilder();
		
	}
	
	@Override
	public void startup() {
		contentBacking = new byte[4];
		
		myAuth = new OAuth1HeaderBuilder(port, "https", host, path);
		myAuth.setupStep3(ck, cs, token, secret);
		
		myAuth.addMACParam("Content-Length", dynamicLength);
		
	}
	
	@Override
	public void run() {
		run(inputs,outputs,toTwitter,fromTwitter);
	}

	private long activeConnectionId=-1;
	private int  activeSequenceId=-1;
	
	private void run( 
			         Pipe<HTTPRequestSchema> input,
			         Pipe<ServerResponseSchema> output,
			         Pipe<ClientHTTPRequestSchema> toTwitter,
			         Pipe<NetResponseSchema> fromTwitter) {
				
		
		while (Pipe.hasContentToRead(fromTwitter) 
			   && Pipe.hasRoomForWrite(output)) {
			
			if (activeSequenceId==-1) {
				logger.info("WARNING: got unrequested response, data skipped");
				Pipe.skipNextFragment(fromTwitter);
			} else {
			
				int msgIdx = Pipe.takeMsgIdx(fromTwitter);
				if (NetResponseSchema.MSG_RESPONSE_101==msgIdx) {
					long connId = Pipe.takeLong(fromTwitter);
					int sessionId = Pipe.takeInt(fromTwitter);
					int flags = Pipe.takeInt(fromTwitter);
					DataInputBlobReader<NetResponseSchema> payload = Pipe.openInputStream(fromTwitter);
					
					final short statusId = payload.readShort();
					
					int contentLength;
					if (200==statusId) {
						contentLength = 0;
					} else {
						contentLength = 1;
						contentBacking[0] = (byte)127;
					}
					
					int channelIdHigh = (int)(activeConnectionId>>32); 
					int channelIdLow = (int)activeConnectionId;
					
					HTTPUtil.publishArrayResponse(ServerCoordinator.END_RESPONSE_MASK | ServerCoordinator.CLOSE_CONNECTION_MASK, 
							      activeSequenceId, statusId, output, channelIdHigh, channelIdLow, null,
							      contentLength, contentBacking, contentPosition, contentMask);          	 
					
				} else {
					Pipe.skipNextFragment(fromTwitter, msgIdx);
					HTTPUtil.publishStatus(activeConnectionId, activeSequenceId, 200, output);
					
				}
			}
			//clear so the next call will not be blocked
			activeConnectionId = -1;
			activeSequenceId = -1;
						
		}
		
		//BillGates		
		//50393960
		//https://127.0.0.1:9443/friendshipCreate?user=1234&friend=5039360
		
		
		
		while (Pipe.hasContentToRead(input) 
				&& activeSequenceId == -1 //only when we have no pending..
				&& Pipe.hasRoomForWrite(output)
				&& Pipe.hasRoomForWrite(toTwitter)) {
						
			    int msgIdx = Pipe.takeMsgIdx(input);
			    switch(msgIdx) {
			        case HTTPRequestSchema.MSG_RESTREQUEST_300:
					
				        long fieldChannelId = Pipe.takeLong(input);
				        int fieldSequence = Pipe.takeInt(input);
				        
				        //store these until we respond
				        activeConnectionId = fieldChannelId;
				        activeSequenceId = fieldSequence;
				        //////////////////
				        				        
				        int fieldVerb = Pipe.takeInt(input);
				        
				        DataInputBlobReader<HTTPRequestSchema> stream = Pipe.openInputStream(input);
				        //read the user id				        
				        long friendUserId = stream.readPackedLong();
						
						int fieldRevision = Pipe.takeInt(input);
						int fieldRequestContext = Pipe.takeInt(input);
												
						processRequest(httpRequestResponseId, output, toTwitter, 
								       friendUserId, 
								       fieldChannelId, 
								       fieldSequence);			
				        
					break;
			        case -1:
			           //requestShutdown();
			        break;
			    }
			    Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
			    Pipe.releaseReadLock(input);
			
		}
	}

	private void processRequest(int httpRequestResponseId, Pipe<ServerResponseSchema> output, Pipe<ClientHTTPRequestSchema> toTwitter,
			long friendUserId, long connectionId, int sequenceID) {
		long now = System.currentTimeMillis();
		
		if (isThisTheTime(now)) {
			
			//do it now, toTwitter then take response and relay it back
			publishRequest(toTwitter, httpRequestResponseId, friendUserId);
			
		} else {
			//too quickly, send back 420
			int contentLength = 1;
			long seconds = ( (nextTriggerTime-now)+1000 )/1000;
			if (seconds>127) { //cap out at 127 seconds
				seconds = 127;
			}
			contentBacking[0] = (byte)seconds;
			
			int channelIdHigh = (int)(connectionId>>32); 
			int channelIdLow = (int)connectionId;
			
			HTTPUtil.publishArrayResponse(ServerCoordinator.END_RESPONSE_MASK | ServerCoordinator.CLOSE_CONNECTION_MASK, 
					      sequenceID, 420, output, channelIdHigh, channelIdLow, null,
					      contentLength, contentBacking, contentPosition, contentMask);

			//clear so the next call will not be blocked
	        activeConnectionId = -1;
	        activeSequenceId = -1;
		}
	}

	private boolean isThisTheTime(long now) {
		if (now > nextTriggerTime) {
			nextTriggerTime = now + msBetweenCalls;
			return true;
		} else {
			return false;
		}
	}

	
	private void publishRequest(Pipe<ClientHTTPRequestSchema> pipe, int httpRequestResponseId, long friendUserId) {
		int sessionId = httpRequestResponseId;
		
		int size = Pipe.addMsgIdx(pipe, ClientHTTPRequestSchema.MSG_GET_200);
		assert(httpRequestResponseId>=0);
		
		Pipe.addIntValue(sessionId, pipe);//session  
		Pipe.addIntValue(port, pipe);                 //port
		int hostId = ClientCoordinator.registerDomain(host);
		Pipe.addIntValue(hostId, pipe);
		Pipe.addLongValue(-1, pipe);
		Pipe.addIntValue(httpRequestResponseId, pipe);//destination
		
		Pipe.addUTF8(path, pipe);
				
		DataOutputBlobWriter<ClientHTTPRequestSchema> stream = Pipe.openOutputStream(pipe);
		
		int bodyLength = prefix.length() + Appendables.appendedLength(friendUserId);
		final boolean isFollow = (path==followPath);
		if (isFollow) {
			bodyLength += postfix.length();
		}
		
		//set the length in the header for the post
		dynamicLength.setLength(0);
		Appendables.appendValue(dynamicLength, bodyLength);
				
		myAuth.addHeaders(stream, "POST").append("\r\n");
	
		stream.append(prefix);
		Appendables.appendValue(stream, friendUserId);
		
		if (isFollow) {
			//user_id=1401881&follow=true
			stream.append(postfix);
		}
		
		DataOutputBlobWriter.closeLowLevelField(stream);

		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		
	}

}
