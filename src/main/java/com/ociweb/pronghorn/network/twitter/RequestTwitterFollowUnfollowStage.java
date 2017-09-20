package com.ociweb.pronghorn.network.twitter;

import com.ociweb.pronghorn.network.OAuth1HeaderBuilder;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RequestTwitterFollowUnfollowStage extends PronghornStage {

	private final String ck;
	private final String cs;
	private final String token;
	private final String secret;
	private OAuth1HeaderBuilder myAuth;
	
	private final int port = 443;
	private final String domain = "api.twitter.com";
	
	//TODO: must not mix these counts.
	private final String followPath   = "/1.1/friendships/create.json";
	private final String unfollowPath = "/1.1/friendships/destroy.json";
	
	/////////////
	//POST https://api.twitter.com/1.1/friendships/create.json?user_id=1401881&follow=true
    //POST https://api.twitter.com/1.1/friendships/destroy.json?user_id=1401881
	/////////////		
	
	public RequestTwitterFollowUnfollowStage(
			GraphManager graphManager,
			String ck, String cs, 
			String token, String secret, 
			Pipe unfollowRequests,   //TODO: server request from somewhere....
			Pipe followRequests,   //TODO: server request from somewhere....
			Pipe<ClientHTTPRequestSchema> output //http request going out  
			) {
		
		super(graphManager, join(unfollowRequests, followRequests), output);
		
		this.ck = ck;
	    this.cs = cs;
		this.token = token;
		this.secret = secret;
			
		int msBetweenCalls = (24*60*60*1000)/1000;      //(15*60*1000)/15; 15 min limit
		assert(86_400 == msBetweenCalls);  //24 hour limit of 1000
		//Schedules may go slower than this rate upon occasion but they are guaranteed not to go faster.
		long nsRate = msBetweenCalls*1_000_000;
		GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, nsRate, this);
	}
	
	@Override
	public void startup() {
		myAuth = new OAuth1HeaderBuilder(ck, cs, token, secret);		
	}
	
	@Override
	public void run() {
		//server request comes in but the response only comes back later
		
		
		//read one request and send response
		
		//TODO: rate must limit this...
		
		
	}

}
