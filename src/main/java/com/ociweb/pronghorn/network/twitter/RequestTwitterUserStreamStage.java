package com.ociweb.pronghorn.network.twitter;

import java.util.ArrayList;
import java.util.List;

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
	
	private final int httpRequestResponseId;	
	
	private static final int port = 443;		
	private static final String host = "userstream.twitter.com";// api.twitter.com";		
	private static final String rawQuery = "stall_warnings=true&with=followings";
	private static final String path2 = "/1.1/user.json";	
	private static final String path = path2+"?"+rawQuery;
	
	public RequestTwitterUserStreamStage(GraphManager graphManager, 
											String ck, String cs, String token, String secret, int httpRequestResponseId,
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
		myAuth = new OAuth1HeaderBuilder(ck, cs, token, secret);
		streamingRequest(output, ck, cs, token, secret, httpRequestResponseId);		
	}
	
	@Override
	public void run() {
		
		//TODO: (a short easy task) twitter wants a growing back-off here, we must record the last time we connected and DO NOT read until sufficient time has passed.
		
		while (PipeReader.tryReadFragment(control)) {			
			int id = PipeReader.getMsgIdx(control);
			switch (id) {
				case -1:
					requestShutdown();
					return;				
				default:
					streamingRequest(output, ck, cs, token, secret, id);
			}			
			PipeReader.releaseReadLock(control);			
		}
	}
	
	@Override
	public void shutdown() {
	}

	private void streamingRequest(Pipe<ClientHTTPRequestSchema> pipe, String ck, String cs, String token, String secret, int httpRequestResponseId) {
			
		PipeWriter.tryWriteFragment(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100);
		PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_DESTINATION_11, httpRequestResponseId);
		PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_SESSION_10, httpRequestResponseId);
		PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PORT_1, port);
		PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2, host);
		PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3, path);
				
		DataOutputBlobWriter<ClientHTTPRequestSchema> stream = PipeWriter.outputStream(pipe);
		DataOutputBlobWriter.openField(stream);
		writeHeaders(stream);
		DataOutputBlobWriter.closeHighLevelField(stream, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HEADERS_7);

		PipeWriter.publishWrites(pipe);
	}

	private void writeHeaders(DataOutputBlobWriter<ClientHTTPRequestSchema> stream) {
		
		//TODO: we have a lot here to improve and eliminate GC.
		
		List<CharSequence[]> javaParams = new ArrayList<CharSequence[]>(2);		
		javaParams.add(new CharSequence[]{"stall_warnings","true"}); //NOTE: must be URLEncoder.encode(
		javaParams.add(new CharSequence[]{"with","followings"}); //NOTE: must be URL encoded
		myAuth.addHeaders(stream, javaParams, port, "https", "GET", host, path2);
		stream.append("\r\n");
	}

}
