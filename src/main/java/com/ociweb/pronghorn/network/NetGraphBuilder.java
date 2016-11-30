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
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.Pool;

public class NetGraphBuilder {
	
	public static void buildHTTPClientGraph(boolean isTLS, GraphManager gm, int maxPartialResponses, ClientConnectionManager ccm,
			IntHashTable listenerPipeLookup, 
			PipeConfig<NetPayloadSchema> clientNetResponseConfig, Pipe<NetPayloadSchema>[] requests,
			Pipe<NetResponseSchema>[] responses) {
		buildHTTPClientGraph(isTLS, gm, maxPartialResponses, ccm, listenerPipeLookup, clientNetResponseConfig, requests, responses, 2, 2);
	}
	
	public static void buildHTTPClientGraph(boolean isTLS, GraphManager gm, int maxPartialResponses, ClientConnectionManager ccm,
			IntHashTable listenerPipeLookup, 
			PipeConfig<NetPayloadSchema> clientNetResponseConfig, Pipe<NetPayloadSchema>[] requests,
			Pipe<NetResponseSchema>[] responses, int responseUnwrapCount, int clientWrapperCount) {
		
		
		PipeConfig<NetParseAckSchema> parseAckConfig = new PipeConfig<NetParseAckSchema>(NetParseAckSchema.instance, 128, 0);
		

		///////////////////
		//add the stage under test
		////////////////////

				
		//the responding reading data is encrypted so there is not much to be tested
		//we will test after the unwrap
		//SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(gm, ccm, socketResponse, clearResponse, false, 0);
		
		Pipe<NetPayloadSchema>[] socketResponse;
		Pipe<NetPayloadSchema>[] clearResponse;
		if (isTLS) {
			//NEED EVEN SPLIT METHOD FOR ARRAY.
			socketResponse = new Pipe[maxPartialResponses];
			clearResponse = new Pipe[maxPartialResponses];		
					
			int k = maxPartialResponses;
			while (--k>=0) {
				socketResponse[k] = new Pipe<NetPayloadSchema>(clientNetResponseConfig);
				clearResponse[k] = new Pipe<NetPayloadSchema>(clientNetResponseConfig.blobGrow2x());
			}
		} else {
			socketResponse = new Pipe[maxPartialResponses];
			clearResponse = socketResponse;		
			
			int k = maxPartialResponses;
			while (--k>=0) {
				socketResponse[k] = new Pipe<NetPayloadSchema>(clientNetResponseConfig);
			}
		}
			
		int a = responseUnwrapCount+1;
		Pipe[] acks = new Pipe[a];
		while (--a>=0) {
			acks[a] =  new Pipe<NetParseAckSchema>(parseAckConfig);	
		}

		ClientSocketReaderStage socketReaderStage = new ClientSocketReaderStage(gm, ccm, acks, socketResponse, isTLS);
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "SocketReader", socketReaderStage);		

		
		if (isTLS) {
						
			int c = responseUnwrapCount;
			Pipe[][] sr = splitPipes(c, socketResponse);
			Pipe[][] cr = splitPipes(c, clearResponse);
			
			while (--c>=0) {
				SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(gm, ccm, sr[c], cr[c], acks[c], false, 0);
				GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
			}
			
		}		
		
		//This is one client routing the responses to multiple listeners
		HTTP1xResponseParserStage parser = new HTTP1xResponseParserStage(gm, clearResponse, responses, acks[acks.length-1], listenerPipeLookup, ccm, HTTPSpecification.defaultSpec());
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "HTTPParser", parser);
		
		//TODO: it may be better to have dedicated handshake stages separate from the wrap/unsrap stages,  look into this for rev 2.
			
		
		//////////////////////////////
		//////////////////////////////
		Pipe<NetPayloadSchema>[] wrappedClientRequests;		
		if (isTLS) {
			wrappedClientRequests = new Pipe[requests.length];	
			int j = requests.length;
			while (--j>=0) {								
				wrappedClientRequests[j] = new Pipe<NetPayloadSchema>(requests[j].config());
			}
			
			int c = clientWrapperCount;			
			Pipe[][] plainData = splitPipes(c, requests);
			Pipe[][] encrpData = splitPipes(c, wrappedClientRequests);
			while (--c>=0) {			
				SSLEngineWrapStage wrapStage = new  SSLEngineWrapStage(gm, ccm, false, plainData[c], encrpData[c], 0 );
				GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "Wrap", wrapStage);
			}
			
		} else {
			wrappedClientRequests = requests;
		}
		//////////////////////////
		///////////////////////////
		
		
		ClientSocketWriterStage socketWriteStage = new ClientSocketWriterStage(gm, ccm, wrappedClientRequests);
		 GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "SocketWriter", socketWriteStage);
		//the data was sent by this stage but the next stage is responsible for responding to the results.
	}

	private static Pipe[][] splitPipes(int pipeCount, Pipe[] socketResponse) {
		
		Pipe[][] result = new Pipe[pipeCount][];
			
		int fullLen = socketResponse.length;
		int last = 0;
		for(int p = 1;p<pipeCount;p++) {			
			int nextLimit = (p*fullLen)/pipeCount;			
			int plen = nextLimit-last;			
		    Pipe[] newPipe = new Pipe[plen];
		    System.arraycopy(socketResponse, last, newPipe, 0, plen);
		    result[p-1]=newPipe;
			last = nextLimit;
		}
		int plen = fullLen-last;
	    Pipe[] newPipe = new Pipe[plen];
	    System.arraycopy(socketResponse, last, newPipe, 0, plen);
	    result[pipeCount-1]=newPipe;
				
		return result;
				
	}
	
	
	public static GraphManager buildHTTPTLSServerGraph(boolean isTLS, GraphManager graphManager, int groups, int maxSimultaniousClients, int apps, ModuleConfig ac, int port) {
        
    	
        PipeConfig<ServerConnectionSchema> newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 10);
        PipeConfig<HTTPRequestSchema> routerToModuleConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, 128, 1<<15);///if paylod is smaller than average file size will be slower
      
        PipeConfig<NetPayloadSchema> incomingDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 6, 1<<15); //Do not make to large or latency goes up

        
        PipeConfig<ServerResponseSchema> outgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 512, 1<<15);//from module to  supervisor        
        PipeConfig<NetPayloadSchema> toWraperConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 256, 1<<15); //from super should be 2x of super input //must be 1<<15 at a minimum for handshake
        
        //olso used when the TLS is not enabled
        PipeConfig<NetPayloadSchema> fromWraperConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 64, 1<<15);  //must be 1<<15 at a minimum for handshake
                
        PipeConfig<NetPayloadSchema> handshakeDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 32, 1<<15); //must be 1<<15 at a minimum for handshake
        
        ServerCoordinator coordinator = new ServerCoordinator(groups, port, maxSimultaniousClients,14);//16K simulanious connections on server. 
        
        //TODO: is the unwrap holding part of the data in its local buffer so it can not be cleared later??
        int requestUnwrapUnits = 4; // NOTE: bump up required for both handshakes and posts, two critical features.
                
        Pipe[][] encryptedIncomingGroup = new Pipe[groups][];
        Pipe[][] planIncomingGroup = new Pipe[groups][];
        Pipe[][] handshakeIncomingGroup = new Pipe[groups][];
        

        int g = groups;
        while (--g >= 0) {//create each connection group            
            
            Pipe<ServerResponseSchema>[] fromModule = new Pipe[apps];
            
            
            Pipe<HTTPRequestSchema>[] toModule = new Pipe[apps];
            
            long[] headers = new long[apps];
            int[] msgIds = new int[apps];
                        
             
            encryptedIncomingGroup[g] = buildPipes(maxSimultaniousClients, incomingDataConfig);
            
            if (isTLS) {
            	planIncomingGroup[g] = buildPipes(maxSimultaniousClients, incomingDataConfig.grow2x());
            } else {
            	planIncomingGroup[g] = encryptedIncomingGroup[g];
            }
            
            
            PipeConfig<NetParseAckSchema> ackConfig = new PipeConfig<NetParseAckSchema>(NetParseAckSchema.instance,2048);
   
            
            int a = requestUnwrapUnits+1;
    		Pipe[] acks = new Pipe[a];
    		while (--a>=0) {
    			acks[a] =  new Pipe<NetParseAckSchema>(ackConfig);	
    		}
                       
            //reads from the socket connection
            ServerConnectionReaderStage readerStage = new ServerConnectionReaderStage(graphManager, acks, encryptedIncomingGroup[g], coordinator, g, isTLS);
            GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketReader", readerStage);
            
               
            if (isTLS) {
            	handshakeIncomingGroup[g] = new Pipe[requestUnwrapUnits];
            	
            	
            	int c = requestUnwrapUnits;
    			Pipe[][] in = splitPipes(c, encryptedIncomingGroup[g]);
    			Pipe[][] out = splitPipes(c, planIncomingGroup[g]);
    			
    			while (--c>=0) {
    				handshakeIncomingGroup[g][c] = new Pipe(handshakeDataConfig);
    				SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(graphManager, coordinator, in[c], out[c], acks[c], handshakeIncomingGroup[g][c], true, 0);
    				GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
    			}
            	
    			//old single
            	//SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(graphManager, coordinator, encryptedIncomingGroup[g], planIncomingGroup[g], handshakeAck, handshakeIncomingGroup[g], true, g);   
            	//   	GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
            }
                        
         
            
            /////////////////////////
            ///////////////////////
            a = apps;
            CharSequence[] paths = new CharSequence[a];
            
            while (--a>=0) { //create every app for this connection group
                
            	fromModule[a] = new Pipe<ServerResponseSchema>(outgoingDataConfig); //TODO: we can have any number of froms URGENT, we need to define elsewhere.
                
                
                toModule[a] =  new Pipe<HTTPRequestSchema>(routerToModuleConfig);
				
                //We only support a single component now, the static file loader
			//	
                headers[a] = ac.addModule(a, graphManager, toModule[a], fromModule[a], HTTPSpecification.defaultSpec());
                
                paths[a] =  ac.getPathRoute(a);//"/%b";  //"/WebSocket/connect",
                
                msgIds[a] =  HTTPRequestSchema.MSG_FILEREQUEST_200;//TODO: add others as needed
            }
            Pipe[] plainPipe = planIncomingGroup[g];//countTap(graphManager, planIncomingGroup[g],"Server-unwrap-to-router");
            HTTP1xRouterStage router = HTTP1xRouterStage.newInstance(graphManager, plainPipe, toModule, acks[acks.length-1], paths, headers, msgIds);        
            GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "HTTPParser", router);
            //////////////////////////
            //////////////////////////
            
            
            int y = 2;//wrapper pipes -- TODO: need more pipes as we have live connections. Give the supervisor a place to store backed up data
            int z = 4;//maxSimultaniousClients;//wrappers
            
            Pipe[] fromSuperPipes = new Pipe[z*y];
            Pipe[] toWiterPipes;
            
            if (isTLS) {
	            
	            toWiterPipes = new Pipe[(z*y) + requestUnwrapUnits ]; //one extra for handshakes if needed
	            int superPos = 0;
	            
	            while (--z>=0) {           
	            	
	            	int w = y;
		            Pipe[] toWrapperPipes = new Pipe[w];
		            Pipe[] fromWrapperPipes = new Pipe[w];            
		            
		            while (--w>=0) {	            	
		            	toWrapperPipes[w] = new Pipe<NetPayloadSchema>(toWraperConfig);
		            	fromWrapperPipes[w] = new Pipe<NetPayloadSchema>(fromWraperConfig); 
		            	toWiterPipes[superPos] = fromWrapperPipes[w];
		            	fromSuperPipes[superPos++] = toWrapperPipes[w];
		            }
		            
		            Pipe[] tapToWrap = toWrapperPipes;//countTap(graphManager, toWrapperPipes,"Server-super-to-wrap");
		            Pipe[] tapFromWrap = fromWrapperPipes;//countTap(graphManager, fromWrapperPipes,"Server-wrap-to-write");
		            
		            SSLEngineWrapStage wrapStage = new SSLEngineWrapStage(graphManager, coordinator, false, tapToWrap, fromWrapperPipes, g);
		            GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "Wrap", wrapStage);
	            }
	            int j = requestUnwrapUnits;
	            while (--j>=0) {
	            	toWiterPipes[superPos++] = handshakeIncomingGroup[g][j]; //handshakes go directly to the socketWriterStage
	            }
	            
	            
            } else {
            	
            	int i = fromSuperPipes.length;
            	while (-- i>= 0) {
            		fromSuperPipes[i]=new Pipe<NetPayloadSchema>(fromWraperConfig);            		
            	}
            	toWiterPipes = fromSuperPipes;      	
            
            }            
            
            Pipe[] tapMod = fromModule;//countTap(graphManager, fromModule, "Server-module-to-supervior");
            
            WrapSupervisorStage wrapSuper = new WrapSupervisorStage(graphManager, tapMod, fromSuperPipes, coordinator);//ensure order           
            
            ServerSocketWriterStage writerStage = new ServerSocketWriterStage(graphManager, toWiterPipes, coordinator, g); //pump bytes out
            GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketWriter", writerStage);
                                                   
        }
              
        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(newConnectionsConfig);
        
        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator, newConnectionsPipe, maxSimultaniousClients);
     //   GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 100, newConStage); 
        
        
        PipeCleanerStage<ServerConnectionSchema> dump = new PipeCleanerStage<>(graphManager, newConnectionsPipe); //IS this important data?

        
      //  GraphManager.exportGraphDotFile(graphManager, "HTTPServer");
    
        
        return graphManager;
    }

//	private static Pipe[] countTap(GraphManager graphManager, Pipe[] tmps, String label) {
//		int q = tmps.length;
//		Pipe[] plainPipe = new Pipe[q];
//		while (--q>=0) {
//			//Tap logic
//			plainPipe[q] = countTap(graphManager, tmps[q], label+" "+tmps[q].id);
//		
//		}
//		return plainPipe;
//	}
//
//	private static <T extends MessageSchema> Pipe<T> countTap(GraphManager gm, Pipe<T> pipe, String label) {
//				
//		Pipe<T> p1 = new Pipe(pipe.config().grow2x());
//		Pipe<T> p2 = new Pipe(pipe.config().grow2x());
//		
//		new ReplicatorStage<T>(gm, pipe, p2, p1);
//		//new ConsoleSummaryStage<>(gm, p1);
//		new PipeCleanerStage<T>(gm, p1, label);
//		return p2;
//	}

	private static Pipe[] buildPipes(int paras, PipeConfig<NetPayloadSchema> incomingDataConfig) {
		
		Pipe[] result = new Pipe[paras];
		int i = paras;
		while (--i>=0) {
			result[i] = new Pipe(incomingDataConfig);
		}
		return result;
	}
	
}
