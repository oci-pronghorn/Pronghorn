package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetParseAckSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
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
			PipeConfig<NetPayloadSchema> clientNetRequestConfig, PipeConfig<NetParseAckSchema> parseAckConfig,
			PipeConfig<NetPayloadSchema> clientNetResponseConfig, Pipe<NetPayloadSchema>[] requests,
			Pipe<NetResponseSchema>[] responses) {
		//this is the fully formed request to be wrapped
		//this is the encrypted (aka wrapped) fully formed requests
		Pipe<NetPayloadSchema>[] wrappedClientRequests = new Pipe[outputsCount];	

		Pipe<NetParseAckSchema> parseAck = new Pipe<NetParseAckSchema>(parseAckConfig);
		Pipe<NetPayloadSchema>[] socketResponse = new Pipe[maxPartialResponses];
		Pipe<NetPayloadSchema>[] clearResponse = new Pipe[maxPartialResponses];		

				
		int k = maxPartialResponses;
		while (--k>=0) {
			socketResponse[k] = new Pipe<NetPayloadSchema>(clientNetResponseConfig);
			clearResponse[k] = new Pipe<NetPayloadSchema>(clientNetResponseConfig);
		}
		
		int j = outputsCount;
		while (--j>=0) {								
			wrappedClientRequests[j] = new Pipe<NetPayloadSchema>(clientNetRequestConfig);
		}
	
		
		///////////////////
		//add the stage under test
		////////////////////
	
		
		SSLEngineWrapStage wrapStage = new  SSLEngineWrapStage(gm,ccm,requests, wrappedClientRequests, 0 );
		//TODO: urgent when we put a delay here the data backs up and we end up with corrupted data.
		//GraphManager.addNota(gm,GraphManager.SCHEDULE_RATE, 200_000,wrapStage);
		
		
		ClientSocketWriterStage socketWriteStage = new ClientSocketWriterStage(gm, ccm, wrappedClientRequests);
		//the data was sent by this stage but the next stage is responsible for responding to the results.
		
		ClientSocketReaderStage socketReaderStage = new ClientSocketReaderStage(gm, ccm, parseAck, socketResponse);
        // 	GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 0, socketReaderStage); //may be required for 10Gb+ connections
				
		//the responding reading data is encrypted so there is not much to be tested
		//we will test after the unwrap
		SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(gm, ccm, socketResponse, clearResponse, false, 0);
	
		//TODO: urgent when we put a delay here the data backs up and we end up with corrupted data.
		//	GraphManager.addNota(gm,GraphManager.SCHEDULE_RATE, 200_000,unwrapStage);
		
		HTTPResponseParserStage parser = new HTTPResponseParserStage(gm, clearResponse, responses, parseAck, listenerPipeLookup, ccm, HTTPSpecification.defaultSpec());
	}

	public static GraphManager buildHTTPServerGraph(GraphManager graphManager, int groups, int apps, ModuleConfig ac) {
	        
	    	
	        PipeConfig<ServerConnectionSchema> newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 10);
	        PipeConfig<HTTPRequestSchema> httpRequestPipeConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, 10, 4000);
	        PipeConfig<ServerResponseSchema> outgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 10, 4000);
	        PipeConfig<NetPayloadSchema> incomingDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 10, 4000);
	        PipeConfig<NetPayloadSchema> socketWriteDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 10, 4000);
	        
	        ServerCoordinator coordinator = new ServerCoordinator(groups, 8081); 
	        
	        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(newConnectionsConfig);
	
	        
	        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator, newConnectionsPipe);
	        PipeCleanerStage<ServerConnectionSchema> dump = new PipeCleanerStage<>(graphManager, newConnectionsPipe);
	        // ConsoleJSONDumpStage dump = new ConsoleJSONDumpStage(graphManager,newConnectionsPipe); //TODO: leave until we resolve the initial hang.
	        
	                
	        Pipe[][] incomingGroup = new Pipe[groups][];
	
	        int g = groups;
	        while (--g >= 0) {//create each connection group            
	            
	            Pipe<ServerResponseSchema>[] fromApps = new Pipe[apps];
	            Pipe<HTTPRequestSchema>[] toApps = new Pipe[apps];
	            
	            long[] headers = new long[apps];
	            int[] msgIds = new int[apps];
	            
	            int a = apps;
	            CharSequence[] paths = new CharSequence[a];
	            
	            while (--a>=0) { //create every app for this connection group
	                fromApps[a] = new Pipe<ServerResponseSchema>(outgoingDataConfig);
	                toApps[a] =  new Pipe<HTTPRequestSchema>(httpRequestPipeConfig);
	                
	                headers[a] = ac.addModule(a, graphManager, toApps[a], fromApps[a], HTTPSpecification.defaultSpec());					
					paths[a] =  ac.getPathRoute(a); //"/%b";  //"/WebSocket/connect",
	                
	                msgIds[a] =  HTTPRequestSchema.MSG_FILEREQUEST_200;//TODO: add others as needed
	            }
	            
	            
	            
	            Pipe<NetPayloadSchema> staticRequestPipe = new Pipe<NetPayloadSchema>(incomingDataConfig);
	            incomingGroup[g] = new Pipe[] {staticRequestPipe};
	            
            
	            //reads from the socket connection
	            ServerConnectionReaderStage readerStage = new ServerConnectionReaderStage(graphManager, incomingGroup[g], coordinator, g, false); //TODO: must take pool 
	            
	            
	            int w = 3;//writers
	            Pipe[] writerPipe = new Pipe[w];
	            while (--w>=0) {	            	
	            	writerPipe[w] = new Pipe<NetPayloadSchema>(socketWriteDataConfig);
	            }
	            WrapSupervisorStage wrapSuper = new WrapSupervisorStage(graphManager, fromApps, writerPipe, coordinator);//ensure order
	            ServerSocketWriterStage writerStage = new ServerSocketWriterStage(graphManager, writerPipe, coordinator, g); //pump bytes out

	            
	            HTTP1xRouterStage.newInstance(graphManager, incomingGroup[g], toApps, paths, headers, msgIds);        
	            
	             
	        }
	    
	        return graphManager;
	    }
	
	
	public static GraphManager buildHTTPTLSServerGraph(GraphManager graphManager, int groups, int apps, ModuleConfig ac, int port) {
        
    	
        PipeConfig<ServerConnectionSchema> newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 10);
        PipeConfig<HTTPRequestSchema> httpRequestPipeConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, 10, 4000);
        PipeConfig<ServerResponseSchema> outgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 10, 4000);
        PipeConfig<NetPayloadSchema> incomingDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 5, 1<<15); //must be 1<<15 at a minimum for handshake
        PipeConfig<NetPayloadSchema> socketWriteDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 5, 1<<15);  //must be 1<<15 at a minimum for handshake      
        PipeConfig<NetPayloadSchema> handshakeDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 2, 1<<15); //must be 1<<15 at a minimum for handshake
        
        ServerCoordinator coordinator = new ServerCoordinator(groups, port); 
        
        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(newConnectionsConfig);

        
        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator, newConnectionsPipe);
        
        
        PipeCleanerStage<ServerConnectionSchema> dump = new PipeCleanerStage<>(graphManager, newConnectionsPipe); //IS this important data?

                
        Pipe[][] encryptedIncomingGroup = new Pipe[groups][];
        Pipe[][] planIncomingGroup = new Pipe[groups][];
        Pipe[][] handshakeIncomingGroup = new Pipe[groups][];
                

        int g = groups;
        while (--g >= 0) {//create each connection group            
            
            Pipe<ServerResponseSchema>[] fromModule = new Pipe[apps];
            
            
            Pipe<HTTPRequestSchema>[] toModule = new Pipe[apps];
            
            long[] headers = new long[apps];
            int[] msgIds = new int[apps];
            
            int a = apps;
            CharSequence[] paths = new CharSequence[a];
            
            while (--a>=0) { //create every app for this connection group
                
            	fromModule[a] = new Pipe<ServerResponseSchema>(outgoingDataConfig); //TODO: we can have any number of froms URGENT, we need to define elsewhere.
                
                
                toModule[a] =  new Pipe<HTTPRequestSchema>(httpRequestPipeConfig);
				
                //We only support a single component now, the static file loader
			//	
                headers[a] = ac.addModule(a, graphManager, toModule[a], fromModule[a], HTTPSpecification.defaultSpec());
                
                paths[a] =  ac.getPathRoute(a);//"/%b";  //"/WebSocket/connect",
                
                msgIds[a] =  HTTPRequestSchema.MSG_FILEREQUEST_200;//TODO: add others as needed
            }
            
            
            Pipe<NetPayloadSchema> rawInputPipe = new Pipe<NetPayloadSchema>(incomingDataConfig);
            encryptedIncomingGroup[g] = new Pipe[] {rawInputPipe};
            
            Pipe<NetPayloadSchema> planInputPipe = new Pipe<NetPayloadSchema>(incomingDataConfig);
            planIncomingGroup[g] = new Pipe[] {planInputPipe};
            
            Pipe<NetPayloadSchema> handshakeInputPipe = new Pipe<NetPayloadSchema>(handshakeDataConfig);
            handshakeIncomingGroup[g] = new Pipe[] {handshakeInputPipe};
                        
            
            //reads from the socket connection
            ServerConnectionReaderStage readerStage = new ServerConnectionReaderStage(graphManager, encryptedIncomingGroup[g], coordinator, g, true);
            
            SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(graphManager, coordinator, encryptedIncomingGroup[g], planIncomingGroup[g], handshakeIncomingGroup[g], true, g);                         
            
            HTTP1xRouterStage.newInstance(graphManager, planIncomingGroup[g], toModule, paths, headers, msgIds);        
         
            
            int w = 3;//wrapers
            Pipe[] toWrapperPipes = new Pipe[w];
            Pipe[] fromWrapperPipes = new Pipe[w];            
            
            while (--w>=0) {	            	
            	toWrapperPipes[w] = new Pipe<NetPayloadSchema>(socketWriteDataConfig);            	
            	fromWrapperPipes[w] = new Pipe<NetPayloadSchema>(socketWriteDataConfig);            	
            }
            
            WrapSupervisorStage wrapSuper = new WrapSupervisorStage(graphManager, fromModule, toWrapperPipes, coordinator);//ensure order           
            
            SSLEngineWrapStage wrapStage = new SSLEngineWrapStage(graphManager, coordinator, toWrapperPipes, fromWrapperPipes, g);

            //hack for now, join these two
            ServerSocketWriterStage handshakeWriterStage = new ServerSocketWriterStage(graphManager, handshakeIncomingGroup[g], coordinator, g); //pump bytes out

            
            ServerSocketWriterStage writerStage = new ServerSocketWriterStage(graphManager, fromWrapperPipes, coordinator, g); //pump bytes out

                                                   
        }
               
        
      //  GraphManager.exportGraphDotFile(graphManager, "HTTPServer");
    
        
        return graphManager;
    }
	
}
