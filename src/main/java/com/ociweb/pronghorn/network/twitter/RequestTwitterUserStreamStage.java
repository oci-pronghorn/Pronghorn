package com.ociweb.pronghorn.network.twitter;

import com.ociweb.pronghorn.network.OAuth1HeaderBuilder;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.TwitterStreamControlSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


public class RequestTwitterUserStreamStage extends PronghornStage {

	private final Pipe<ClientHTTPRequestSchema> output;
	private final Pipe<TwitterStreamControlSchema> control;
	
	private final String ck;
	private final String cs;
	private final String token;
	private final String secret;
	private OAuth1HeaderBuilder myAuth;
	private String hostAndPath;
	
	private final int httpRequestResponseId;	
	
	private static final int port = 443;		
	private static final String host = "userstream.twitter.com";// api.twitter.com";		
	private static final String rawQuery = "stall_warnings=true&with=followings";
	private static final String pathRoot = "/1.1/user.json";	
	private static final String path = pathRoot+"?"+rawQuery;
	
	public RequestTwitterUserStreamStage(GraphManager graphManager, 
											String ck, String cs, 
											String token, String secret, 
											int httpRequestResponseId,
											Pipe<TwitterStreamControlSchema> control,
			                                Pipe<ClientHTTPRequestSchema> output) {
		
		super(graphManager, control, output);
		this.control = control;
		this.output = output;
		
		this.ck = ck;
		this.cs = cs;
		this.token = token;
		this.secret = secret;
		this.httpRequestResponseId = httpRequestResponseId;		
	}
	
	@Override
	public void startup() {
		
		myAuth = new OAuth1HeaderBuilder(ck, port, "https", host, pathRoot);
	    myAuth.setupStep3(cs, token, secret);
		
		myAuth.addMACParam("stall_warnings","true");
		myAuth.addMACParam("with","followings");		
		
		streamingRequest(output, httpRequestResponseId);		
	}
	
	@Override
	public void run() {
		
		//TODO: (a short easy task) twitter wants a growing back-off here, we must record the last time we connected and DO NOT read until sufficient time has passed.
		
		while (PipeWriter.hasRoomForWrite(output) &&
				PipeReader.tryReadFragment(control)) {			
			int id = PipeReader.getMsgIdx(control);
			switch (id) {
				case -1:
					requestShutdown();
					return;				
				default:
					streamingRequest(output, id);
			}			
			PipeReader.releaseReadLock(control);			
		}
	}
	
	@Override
	public void shutdown() {
	}

	private void streamingRequest(Pipe<ClientHTTPRequestSchema> pipe, int httpRequestResponseId) {
			
		PipeWriter.presumeWriteFragment(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100);
		assert(httpRequestResponseId>=0);
		PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_DESTINATION_11, httpRequestResponseId);
		PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_SESSION_10, httpRequestResponseId);
		PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PORT_1, port);
		PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2, host);
		PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3, path);
				
		DataOutputBlobWriter<ClientHTTPRequestSchema> stream = PipeWriter.outputStream(pipe);
		DataOutputBlobWriter.openField(stream);
		myAuth.addHeaders(stream, "GET").append("\r\n");
		DataOutputBlobWriter.closeHighLevelField(stream, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HEADERS_7);

		PipeWriter.publishWrites(pipe);
	}

}
