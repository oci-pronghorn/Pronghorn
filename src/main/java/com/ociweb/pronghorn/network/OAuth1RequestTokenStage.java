package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

public class OAuth1RequestTokenStage extends PronghornStage {

	private final Pipe<HTTPRequestSchema>[] inputs; 
    private final Pipe<ClientHTTPRequestSchema> clientRequestsPipe;
    
    private OAuth1HeaderBuilder oauth;
    
	private static final String scheme   = "https";
	private static final int    port     = 443;		
	private static final String host     = "userstream.twitter.com";// api.twitter.com";		
	private static final String pathRoot = "/1.1/user.json";
	private final HTTPSpecification<?, ?, ?, ?> httpSpec;
	
	
	public OAuth1RequestTokenStage(GraphManager graphManager, 
			                       Pipe<HTTPRequestSchema>[] inputs, 
			                       Pipe<ClientHTTPRequestSchema> clientRequestsPipe,
			                       int responseId,
			                       HTTPSpecification<?, ?, ?, ?> httpSpec) {
		
		super(graphManager, inputs, clientRequestsPipe);
		this.inputs = inputs;
		this.clientRequestsPipe = clientRequestsPipe;
		this.httpSpec = httpSpec;
		
	}

	@Override
	public void startup() {
		this.oauth = new OAuth1HeaderBuilder(port, scheme, host, pathRoot);
		
	}
	
	@Override
	public void run() {
		
		int i = inputs.length;
		while (Pipe.hasRoomForWrite(clientRequestsPipe)  && --i>=0) {
			sendRequest(inputs[i]);
		}
		
	}

	private void sendRequest(Pipe<HTTPRequestSchema> pipe) {
		
		
		while (Pipe.hasContentToRead(pipe)) {
			int msgIdx = Pipe.takeMsgIdx(pipe);
			
		    long fieldChannelId = Pipe.takeLong(pipe);
 	        int fieldSequence = Pipe.takeInt(pipe);
		    int fieldVerb = Pipe.takeInt(pipe);
		    
		    DataInputBlobReader<HTTPRequestSchema> stream = Pipe.openInputStream(pipe);
		    
		    int status = stream.readShort();
		    		    
		    
			int headerId = stream.readShort();
			
			while (-1 != headerId) { //end of headers will be marked with -1 value
						
		
				System.out.println("first call ");
				System.out.print(httpSpec.headers[headerId]);
				System.out.print(" ");
				httpSpec.writeHeader(System.out, headerId, stream);
				System.out.println();
							
				
				//read next
				headerId = stream.readShort();
			}
		    
		    
            //Need to read the user ID?
		    String consumerKey = stream.readUTF(); //TODO: need to skip headers??
		    
		    System.out.println("consumer key is: "+consumerKey);
		    
		    oauth.setupStep1(consumerKey);

		    
		    int fieldRevision = Pipe.takeInt(pipe);
		    int fieldRequestContext = Pipe.takeInt(pipe);
		    
		    //TODO: write the request
		    //oauth.addHeaders(builder, upperVerb);
		    
		    //TODO: get the response and dump it to console

		    Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
		    Pipe.releaseReadLock(pipe);
		    
			
		}
		
		
		
		//String consumerKey = ""; //pass this consumer key to get the RequestKey
		//this.oauth.setupStep1(consumerKey);
		
		
		
	}

}
