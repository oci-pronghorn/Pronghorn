package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.ClientNetRequestSchema;
import com.ociweb.pronghorn.network.schema.ClientNetResponseSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetParseAckSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.ServerRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.Pool;

public class NetGraphBuilder {

	
	public static void buildHTTPClientGraph(GraphManager gm, int outputsCount, int maxPartialResponses,
			ClientConnectionManager ccm, IntHashTable listenerPipeLookup,
			PipeConfig<ClientNetRequestSchema> clientNetRequestConfig, PipeConfig<NetParseAckSchema> parseAckConfig,
			PipeConfig<ClientNetResponseSchema> clientNetResponseConfig, Pipe<ClientNetRequestSchema>[] requests,
			Pipe<NetResponseSchema>[] responses) {
		//this is the fully formed request to be wrapped
		//this is the encrypted (aka wrapped) fully formed requests
		Pipe<ClientNetRequestSchema>[] wrappedClientRequests = new Pipe[outputsCount];	

		Pipe<NetParseAckSchema> parseAck = new Pipe<NetParseAckSchema>(parseAckConfig);
		Pipe<ClientNetResponseSchema>[] socketResponse = new Pipe[maxPartialResponses];
		Pipe<ClientNetResponseSchema>[] clearResponse = new Pipe[maxPartialResponses];		

				
		int k = maxPartialResponses;
		while (--k>=0) {
			socketResponse[k] = new Pipe<ClientNetResponseSchema>(clientNetResponseConfig);
			clearResponse[k] = new Pipe<ClientNetResponseSchema>(clientNetResponseConfig);
		}
		
		int j = outputsCount;
		while (--j>=0) {								
			wrappedClientRequests[j] = new Pipe<ClientNetRequestSchema>(clientNetRequestConfig);
		}
	
		
		///////////////////
		//add the stage under test
		////////////////////
	
		
		SSLEngineWrapStage wrapStage = new  SSLEngineWrapStage(gm,ccm,requests, wrappedClientRequests );
		//TODO: urgent when we put a delay here the data backs up and we end up with corrupted data.
		//GraphManager.addNota(gm,GraphManager.SCHEDULE_RATE, 200_000,wrapStage);
		
		
		ClientSocketWriterStage socketWriteStage = new ClientSocketWriterStage(gm, ccm, wrappedClientRequests);
		//the data was sent by this stage but the next stage is responsible for responding to the results.
		
		ClientSocketReaderStage socketReaderStage = new ClientSocketReaderStage(gm, ccm, parseAck, socketResponse);
        // 	GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 0, socketReaderStage); //may be required for 10Gb+ connections
				
		//the responding reading data is encrypted so there is not much to be tested
		//we will test after the unwrap
		SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(gm, ccm, socketResponse, clearResponse);
	
		//TODO: urgent when we put a delay here the data backs up and we end up with corrupted data.
		//	GraphManager.addNota(gm,GraphManager.SCHEDULE_RATE, 200_000,unwrapStage);
		
		HTTPResponseParserStage parser = new HTTPResponseParserStage(gm, clearResponse, responses, parseAck, listenerPipeLookup, ccm, HTTPSpecification.defaultSpec());
	}

	//TODO: build all the application
	public static long newApp(GraphManager graphManager, Pipe<HTTPRequestSchema> fromRequest, Pipe<ServerResponseSchema>  toSend, int appId) {
	    
	    //TODO: build apps to connect these
	    //Static file load
	    //RestCall
	    //WebSocketAPI.
	    
	    
	    //We only support a single component now, the static file loader
	    HTTPModuleFileReadStage.newInstance(graphManager, fromRequest, toSend, HTTPSpecification.defaultSpec(), "/home/nate/elmForm");
	    
	    return 0;
	}

	public static GraphManager buildHTTPServerGraph(GraphManager graphManager, int groups, int apps) {
	        
	    	
	        PipeConfig<ServerConnectionSchema> newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 10);
	        PipeConfig<HTTPRequestSchema> httpRequestPipeConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, 10, 4000);
	        PipeConfig<ServerResponseSchema> outgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 10, 4000);
	        PipeConfig<ServerRequestSchema> incomingDataConfig = new PipeConfig<ServerRequestSchema>(ServerRequestSchema.instance, 10, 4000);
	    	
	        ServerCoordinator coordinator = new ServerCoordinator(groups, 8081); 
	        
	        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(newConnectionsConfig);
	
	        
	        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator, newConnectionsPipe);
	        //PipeCleanerStage<ServerConnectionSchema> dump = new PipeCleanerStage<>(graphManager, newConnectionsPipe);
	        ConsoleJSONDumpStage dump = new ConsoleJSONDumpStage(graphManager,newConnectionsPipe); //TODO: leave until we resolve the initial hang.
	        
	                
	        Pipe[][] incomingGroup = new Pipe[groups][];
	
	        int g = groups;
	        while (--g >= 0) {//create each connection group            
	            
	            Pipe<ServerResponseSchema>[] fromApps = new Pipe[apps];
	            Pipe<HTTPRequestSchema>[] toApps = new Pipe[apps];
	            
	            long[] headers = new long[apps];
	            int[] msgIds = new int[apps];
	            
	            int a = apps;
	            while (--a>=0) { //create every app for this connection group
	                fromApps[a] = new Pipe<ServerResponseSchema>(outgoingDataConfig);
	                toApps[a] =  new Pipe<HTTPRequestSchema>(httpRequestPipeConfig);
	                headers[a] = newApp(graphManager, toApps[a], fromApps[a], a);
	                msgIds[a] =  HTTPRequestSchema.MSG_FILEREQUEST_200;//TODO: add others as needed
	            }
	            
	            CharSequence[] paths = new CharSequence[] {
	            											"/WebSocket/connect",
	            											"/%b"};
	            
	            
	            
	            Pipe<ServerRequestSchema> staticRequestPipe = new Pipe<ServerRequestSchema>(incomingDataConfig);
	            incomingGroup[g] = new Pipe[] {staticRequestPipe};
	            
	            Pool<Pipe<ServerRequestSchema>> pool = new Pool<Pipe<ServerRequestSchema>>(incomingGroup[g]);
	            
	                        
	            //TODO: revisit this part of the test later.
	            Pipe errorPipe = new Pipe(new PipeConfig(RawDataSchema.instance));
	            ConsoleJSONDumpStage dumpErr = new ConsoleJSONDumpStage(graphManager,errorPipe); //TODO: Build error creation stage. what data can we know here?
	            
	            
	            //reads from the socket connection
	            ServerConnectionReaderStage readerStage = new ServerConnectionReaderStage(graphManager, incomingGroup[g], coordinator, g); //TODO: must take pool 
	            
	            
	            HTTPRouterStage.newInstance(graphManager, pool, toApps, errorPipe, paths, headers, msgIds);        
	            
	            
	//            int j = fromApps.length;
	//            while (--j>=0) {
	//            	ConsoleJSONDumpStage<?> stage = new ConsoleJSONDumpStage(graphManager, fromApps[j]); //TODO: make version that takes array
	//            }
	            
	                        
	            //writes to the socket connection           
	            ServerConnectionWriterStage writerStage = new ServerConnectionWriterStage(graphManager, fromApps, coordinator, g); 
	                                       
	        }
	               
	        
	      //  GraphManager.exportGraphDotFile(graphManager, "HTTPServer");
	    
	        
	        return graphManager;
	    }
	
}
