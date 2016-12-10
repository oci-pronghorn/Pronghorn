package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.Pool;

public class NetGraphBuilder {
	
	public static void buildHTTPClientGraph(boolean isTLS, GraphManager gm, int maxPartialResponses, ClientCoordinator ccm,
			IntHashTable listenerPipeLookup, 
			int responseQueue, int responseSize, Pipe<NetPayloadSchema>[] requests,
			Pipe<NetResponseSchema>[] responses) {
		buildHTTPClientGraph(isTLS, gm, maxPartialResponses, ccm, listenerPipeLookup, responseQueue, responseSize, requests, responses, 2, 2);
	}
	
	public static void buildHTTPClientGraph(boolean isTLS, GraphManager gm, int maxPartialResponses, ClientCoordinator ccm,
			IntHashTable listenerPipeLookup, 
			int responseQueue, int responseSize, Pipe<NetPayloadSchema>[] requests,
			Pipe<NetResponseSchema>[] responses, int responseUnwrapCount, int clientWrapperCount) {
		
		
		PipeConfig<ReleaseSchema> parseAckConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance, 2048, 0);
		

		//must be large enough for handshake plus this is the primary pipe after the socket so it must be a little larger.
		PipeConfig<NetPayloadSchema> clientNetResponseConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, responseQueue, responseSize); 	
		
		
		
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
			
		int a = 1 + (isTLS?responseUnwrapCount:0);
		Pipe[] acks = new Pipe[a];
		while (--a>=0) {
			acks[a] =  new Pipe<ReleaseSchema>(parseAckConfig);	
		}

		ClientSocketReaderStage socketReaderStage = new ClientSocketReaderStage(gm, ccm, acks, socketResponse, isTLS);
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "SocketReader", socketReaderStage);		

		
		Pipe[] hanshakePipes = null;
		if (isTLS) {
						
			int c = responseUnwrapCount;
			Pipe[][] sr = Pipe.splitPipes(c, socketResponse);
			Pipe[][] cr = Pipe.splitPipes(c, clearResponse);
			
			hanshakePipes = new Pipe[c];
			
			while (--c>=0) {
				hanshakePipes[c] = new Pipe<NetPayloadSchema>(requests[0].config()); 
				SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(gm, ccm, sr[c], cr[c], acks[c], hanshakePipes[c], false, 0);
				GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
			}
			
		}		
		
		//This is one client routing the responses to multiple listeners
		HTTP1xResponseParserStage parser = new HTTP1xResponseParserStage(gm, clearResponse, responses, acks[acks.length-1], listenerPipeLookup, ccm, HTTPSpecification.defaultSpec());
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "HTTPParser", parser);
			
		
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
			Pipe[][] plainData = Pipe.splitPipes(c, requests);
			Pipe[][] encrpData = Pipe.splitPipes(c, wrappedClientRequests);
			while (--c>=0) {			
				if (encrpData[c].length>0) {
					SSLEngineWrapStage wrapStage = new  SSLEngineWrapStage(gm, ccm, false, plainData[c], encrpData[c], 0 );
					GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "Wrap", wrapStage);
				}
			}
			
		} else {
			wrappedClientRequests = requests;
		}
		//////////////////////////
		///////////////////////////
		
		if (isTLS) { //add in the handshake pipes.
			wrappedClientRequests = PronghornStage.join(wrappedClientRequests,hanshakePipes);
		}
		
		ClientSocketWriterStage socketWriteStage = new ClientSocketWriterStage(gm, ccm, wrappedClientRequests);
	    GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "SocketWriter", socketWriteStage);
		
	}

	
	
	//TODO: review each stage for opportunities to use batch combine logic, (eg n in a row). May go a lot faster for single power users. future feature.
	
	public static GraphManager buildHTTPServerGraph(boolean isTLS, GraphManager graphManager, int groups,
			int maxSimultaniousClients, int apps, ModuleConfig ac, ServerCoordinator coordinator, int requestUnwrapUnits, int responseWrapUnits, int pipesPerWrapUnit, int socketWriters) {

		PipeConfig<ServerConnectionSchema> newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 10);
        PipeConfig<HTTPRequestSchema> routerToModuleConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, 1024, 1<<15);///if payload is smaller than average file size will be slower
      
        //byte buffer must remain small because we will have a lot of these for all the partial messages
        PipeConfig<NetPayloadSchema> incomingDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 2, 1<<15); //Do not make to large or latency goes up
        
        PipeConfig<ServerResponseSchema> outgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 2048, 1<<15);//from module to  supervisor
        
        //must be large to hold high volumes of throughput.  //NOTE: effeciency of supervisor stage controls how long this needs to be
        PipeConfig<NetPayloadSchema> toWraperConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 1024, 1<<15); //from super should be 2x of super input //must be 1<<15 at a minimum for handshake
        
        //olso used when the TLS is not enabled                 must be less than the outgoing buffer size of socket?
        PipeConfig<NetPayloadSchema> fromWraperConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 512, 1<<17);  //must be 1<<15 at a minimum for handshake
                
        PipeConfig<NetPayloadSchema> handshakeDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 32, 1<<15); //must be 1<<15 at a minimum for handshake
        
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
            
            
            PipeConfig<ReleaseSchema> ackConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance,2048);
   
            
            int a = 1+(isTLS?requestUnwrapUnits:0);
    		Pipe[] acks = new Pipe[a];
    		while (--a>=0) {
    			acks[a] =  new Pipe<ReleaseSchema>(ackConfig);	
    		}
                       
            //reads from the socket connection
            ServerSocketReaderStage readerStage = new ServerSocketReaderStage(graphManager, acks, encryptedIncomingGroup[g], coordinator, g, isTLS);
            GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketReader", readerStage);
            
               
            if (isTLS) {
            	handshakeIncomingGroup[g] = new Pipe[requestUnwrapUnits];
            	
            	
            	int c = requestUnwrapUnits;
    			Pipe[][] in = Pipe.splitPipes(c, encryptedIncomingGroup[g]);
    			Pipe[][] out = Pipe.splitPipes(c, planIncomingGroup[g]);
    			
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
                
                msgIds[a] =  HTTPRequestSchema.MSG_FILEREQUEST_200;
            }
            Pipe[] plainPipe = planIncomingGroup[g];//countTap(graphManager, planIncomingGroup[g],"Server-unwrap-to-router");
            HTTP1xRouterStage router = HTTP1xRouterStage.newInstance(graphManager, plainPipe, toModule, acks[acks.length-1], paths, headers, msgIds);        
            GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "HTTPParser", router);
            //////////////////////////
            //////////////////////////
          
            
            int y = pipesPerWrapUnit;
            int z = responseWrapUnits;
            
            Pipe[] fromSuperPipes = new Pipe[z*y];
            Pipe[] toWiterPipes;
            
            if (isTLS) {
	            
	            toWiterPipes = new Pipe[(z*y) + requestUnwrapUnits ]; //one extra for handshakes if needed
	            
	            int toWriterPos = 0;
	            int fromSuperPos = 0;
	            
	            int remHanshakePipes = requestUnwrapUnits;
	            
	            while (--z>=0) {           
	            	
	            	//as possible we must mix up the pipes to ensure handshakes go to different writers.
		            if (--remHanshakePipes>=0) {
		            	toWiterPipes[toWriterPos++] = handshakeIncomingGroup[g][remHanshakePipes]; //handshakes go directly to the socketWriterStage
		            }
	            	
	            	//
	            	int w = y;
		            Pipe[] toWrapperPipes = new Pipe[w];
		            Pipe[] fromWrapperPipes = new Pipe[w];            
		            
		            while (--w>=0) {	
		            	toWrapperPipes[w] = new Pipe<NetPayloadSchema>(toWraperConfig);
		            	fromWrapperPipes[w] = new Pipe<NetPayloadSchema>(fromWraperConfig); 
		            	toWiterPipes[toWriterPos++] = fromWrapperPipes[w];
		            	fromSuperPipes[fromSuperPos++] = toWrapperPipes[w];
		            }
		            
		            Pipe[] tapToWrap = toWrapperPipes;//countTap(graphManager, toWrapperPipes,"Server-super-to-wrap");
		            Pipe[] tapFromWrap = fromWrapperPipes;//countTap(graphManager, fromWrapperPipes,"Server-wrap-to-write");
		            
		            SSLEngineWrapStage wrapStage = new SSLEngineWrapStage(graphManager, coordinator, false, tapToWrap, fromWrapperPipes, g);
		            GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "Wrap", wrapStage);
	            }
	            
	            //finish up any remaning handshakes
	            while (--remHanshakePipes>=0) {
	            	toWiterPipes[toWriterPos++] = handshakeIncomingGroup[g][remHanshakePipes]; //handshakes go directly to the socketWriterStage
	            }
	            
	            
            } else {
            	
            	int i = fromSuperPipes.length;
            	while (-- i>= 0) {
            		fromSuperPipes[i]=new Pipe<NetPayloadSchema>(fromWraperConfig);            		
            	}
            	toWiterPipes = fromSuperPipes;      	
            
            }            
            
            Pipe[] tapMod = fromModule;//countTap(graphManager, fromModule, "Server-module-to-supervior");
            
            WrapSupervisorStage wrapSuper = new WrapSupervisorStage(graphManager, tapMod, fromSuperPipes, coordinator, isTLS);//ensure order           
            
            
            Pipe[][] req = Pipe.splitPipes(socketWriters, toWiterPipes);	
            int w = socketWriters;
            while (--w>=0) {
            	ServerSocketWriterStage writerStage = new ServerSocketWriterStage(graphManager, coordinator, req[w], g); //pump bytes out
                GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketWriter", writerStage);
               	
            }
           
        }
              
        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(newConnectionsConfig);        
        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator, newConnectionsPipe, isTLS); 
        PipeCleanerStage<ServerConnectionSchema> dump = new PipeCleanerStage<>(graphManager, newConnectionsPipe); //IS this important data?
        
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
