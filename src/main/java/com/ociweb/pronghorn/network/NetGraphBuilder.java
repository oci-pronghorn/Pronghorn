package com.ociweb.pronghorn.network;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	private static final Logger logger = LoggerFactory.getLogger(NetGraphBuilder.class);	
	
	public static void buildHTTPClientGraph(boolean isTLS, GraphManager gm, int maxPartialResponses, ClientCoordinator ccm,
			IntHashTable listenerPipeLookup, 
			int responseQueue, int responseSize, Pipe<NetPayloadSchema>[] requests,
			Pipe<NetResponseSchema>[] responses) {
		buildHTTPClientGraph(isTLS, gm, maxPartialResponses, ccm, listenerPipeLookup, responseQueue, responseSize, requests, responses, 2, 2, 2);
	}
	
	public static void buildHTTPClientGraph(boolean isTLS, GraphManager gm, int maxPartialResponses, ClientCoordinator ccm,
			IntHashTable listenerPipeLookup, 
			int responseQueue, int responseSize, Pipe<NetPayloadSchema>[] requests,
			Pipe<NetResponseSchema>[] responses, int responseUnwrapCount, int clientWrapperCount, int clientWriters
			) {
		
		int httpResponseSize = 64;
		int httpResponseBlob = 1<<19;
		
		int releaseSize = 2048;
		
		PipeConfig<ReleaseSchema> parseReleaseConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance, releaseSize, 0);
		

		//must be large enough for handshake plus this is the primary pipe after the socket so it must be a little larger.
		PipeConfig<NetPayloadSchema> clientNetResponseConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, responseQueue, responseSize); 	
		
		
		//pipe holds data as it is parsed so making it larger is helpfull
		PipeConfig<NetPayloadSchema> clientHTTPResponseConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, httpResponseSize, httpResponseBlob); 	
		
		
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
				socketResponse[k] = new Pipe<NetPayloadSchema>(clientNetResponseConfig,false);
				clearResponse[k] = new Pipe<NetPayloadSchema>(clientHTTPResponseConfig,false);
			}
		} else {
			socketResponse = new Pipe[maxPartialResponses];
			clearResponse = socketResponse;		
			
			int k = maxPartialResponses;
			while (--k>=0) {
				socketResponse[k] = new Pipe<NetPayloadSchema>(clientHTTPResponseConfig,false);
			}
		}
			
		final int responseParsers = 1; //NOTE: can not be changed because only 1 can bs supported
		
		int a = responseParsers + (isTLS?responseUnwrapCount:0);
		Pipe[] acks = new Pipe[a];
		while (--a>=0) {
			acks[a] =  new Pipe<ReleaseSchema>(parseReleaseConfig,false);	
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
				hanshakePipes[c] = new Pipe<NetPayloadSchema>(requests[0].config(),false); 
				SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(gm, ccm, sr[c], cr[c], acks[c], hanshakePipes[c], false, 0);
				GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
			}
			
		}		
		
		HTTP1xResponseParserStage parser = new HTTP1xResponseParserStage(gm, clearResponse, responses, acks[acks.length-1], listenerPipeLookup, ccm, HTTPSpecification.defaultSpec());
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "HTTPParser", parser);

		
		
		//////////////////////////////
		//////////////////////////////
		Pipe<NetPayloadSchema>[] wrappedClientRequests;		
		if (isTLS) {
			wrappedClientRequests = new Pipe[requests.length];	
			int j = requests.length;
			while (--j>=0) {								
				wrappedClientRequests[j] = new Pipe<NetPayloadSchema>(requests[j].config(),false);
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
			
			//change order of pipes for split later
			//interleave the handshakes.
			c = hanshakePipes.length;
			Pipe[][] tPipes = Pipe.splitPipes(c, wrappedClientRequests);
			while (--c>=0) {
				tPipes[c] = PronghornStage.join(tPipes[c], hanshakePipes[c]);
			}
			wrappedClientRequests = PronghornStage.join(tPipes);
			////////////////////////////
				
			
			
		} else {
			wrappedClientRequests = requests;
		}
		//////////////////////////
		///////////////////////////
		
		
		
		Pipe[][] clientRequests = Pipe.splitPipes(clientWriters, wrappedClientRequests);
		
		int i = clientWriters;
		
		while (--i>=0) {		
			ClientSocketWriterStage socketWriteStage = new ClientSocketWriterStage(gm, ccm, clientRequests[i]);
	    	GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "SocketWriter", socketWriteStage);
	    	//GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 100_000_000, socketWriteStage);//slow down writers.
	    	
		}	    
	    
	    
	}

	
	
	//TODO: review each stage for opportunities to use batch combine logic, (eg n in a row). May go a lot faster for single power users. future feature.
	
	public static GraphManager buildHTTPServerGraph(boolean isTLS, GraphManager graphManager, int groups,
			int maxSimultanious, ModuleConfig ac, ServerCoordinator coordinator, int requestUnwrapUnits, int responseWrapUnits, 
			int pipesPerWrapUnit, int socketWriters, int serverInputMsg, int serverInputBlobs, 
			int serverMsgToEncrypt, int serverBlobToEncrypt, int serverMsgToWrite, int serverBlobToWrite, int routerCount, 
			int fromRouterMsg, int fromRouterBlob, int releaseMsg) {
		
        
        
        PipeConfig<ReleaseSchema> releaseConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance,releaseMsg);
        
		PipeConfig<ServerConnectionSchema> newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 10);
		
		//Why? the router is helped with lots of extra room for write?  - may need to be bigger for posts.
        PipeConfig<HTTPRequestSchema> routerToModuleConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, fromRouterMsg, fromRouterBlob);///if payload is smaller than average file size will be slower
      
        //byte buffer must remain small because we will have a lot of these for all the partial messages
        //TODO: if we get a series of very short messages this will fill up causing a hang. TODO: we can get parser to release and/or server reader to combine.
        PipeConfig<NetPayloadSchema> incomingDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, serverInputMsg, serverInputBlobs);//make larger if we are suporting posts. 1<<20); //Make same as network buffer in bytes!??   Do not make to large or latency goes up
        
        //must be large to hold high volumes of throughput.  //NOTE: effeciency of supervisor stage controls how long this needs to be
        PipeConfig<NetPayloadSchema> toWraperConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, serverMsgToEncrypt, serverBlobToEncrypt); //from super should be 2x of super input //must be 1<<15 at a minimum for handshake
        
        //also used when the TLS is not enabled                 must be less than the outgoing buffer size of socket?
        PipeConfig<NetPayloadSchema> fromWraperConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, serverMsgToWrite, serverBlobToWrite);  //must be 1<<15 at a minimum for handshake
                
        PipeConfig<NetPayloadSchema> handshakeDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, maxSimultanious>>1, 1<<15); //must be 1<<15 at a minimum for handshake
        
        Pipe[][] encryptedIncomingGroup = new Pipe[groups][];
        Pipe[][] planIncomingGroup = new Pipe[groups][];
        Pipe[][] handshakeIncomingGroup = new Pipe[groups][];
        
        if (ac.moduleCount()==0) {
        	throw new UnsupportedOperationException("Must be using at least 1 module to startup.");
        }
        
        
        int g = groups;
        while (--g >= 0) {//create each connection group            
             
            encryptedIncomingGroup[g] = buildPipes(maxSimultanious, incomingDataConfig);
            
            if (isTLS) {
            	planIncomingGroup[g] = buildPipes(maxSimultanious, incomingDataConfig);
            } else {
            	planIncomingGroup[g] = encryptedIncomingGroup[g];
            }
    
               
            int a = routerCount+(isTLS?requestUnwrapUnits:0);
    		Pipe[] acks = new Pipe[a];
    		while (--a>=0) {
    			acks[a] =  new Pipe<ReleaseSchema>(releaseConfig, false);	
    		}
                       
            //reads from the socket connection
            ServerSocketReaderStage readerStage = new ServerSocketReaderStage(graphManager, acks, encryptedIncomingGroup[g], coordinator, g, isTLS);
            GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketReader", readerStage);
                           
            if (isTLS) {
            	populateGraphWithUnWrapStages(graphManager, coordinator, requestUnwrapUnits, handshakeDataConfig,
            			                      encryptedIncomingGroup, planIncomingGroup, handshakeIncomingGroup, g, acks);
            }                                 
            
            /////////////////////////
            ///////////////////////
            a = ac.moduleCount();

            //split up the unencrypted pipes across all the routers
            Pipe[][] plainSplit = Pipe.splitPipes(routerCount, planIncomingGroup[g]);
            

            //create the modules
                                 
            
            Pipe<HTTPRequestSchema>[][] toModules = new Pipe[ac.moduleCount()][routerCount];
            Pipe<ServerResponseSchema>[][][] fromModule = new Pipe[routerCount][ac.moduleCount()][]; 
            
            long[] headers = new long[ac.moduleCount()];
            CharSequence[][] paths = new CharSequence[1][ac.moduleCount()];
  
            while (--a >= 0) { //create every app for this connection group   
            		            	
            	int r = routerCount;
            	while (--r >= 0) {
            		toModules[a][r] =  new Pipe<HTTPRequestSchema>(routerToModuleConfig,false);		
            	}            	
                headers[a]    = ac.addModule(a, graphManager, toModules[a], HTTPSpecification.defaultSpec());  
                
                
                Pipe<ServerResponseSchema>[][] outputPipes = ac.outputPipes(a);
                int i = outputPipes.length;
                assert(i==routerCount);
      
                while (--i>=0) {
                	fromModule[i][a] = outputPipes[i];  
                }    
                
                paths[0][a]      = ac.getPathRoute(a);//"/%b";  //"/WebSocket/connect",                
 
            }

            
            ////////////////
            ////////////////
            
            final HTTP1xRouterStageConfig routerConfig = new HTTP1xRouterStageConfig(paths[0], headers, HTTPSpecification.defaultSpec()); 
 
            //create the routers
            int acksBase = acks.length-1;
            int r = routerCount;
            while (--r>=0) {
            	
            	a = ac.moduleCount();
            	Pipe<HTTPRequestSchema>[][] toAllModules = new Pipe[1][a];
            	while (--a>=0) {           		

            			toAllModules[0][a] = toModules[a][r]; //cross cut this matrix
         		
            	}
            	
            	//the router knows the index of toAllModules matches the index of the appropriate path.
            	assert(toAllModules.length == paths.length);
            	
            	
            	//TODO: must provide multiple request pipes. mod these by the connection id
            	
            	
            	HTTP1xRouterStage router = HTTP1xRouterStage.newInstance(graphManager, plainSplit[r], toAllModules, acks[acksBase-r], routerConfig);        
            	GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "HTTPParser", router);
            	
            }
            
            
            
            //////////////////////////
            //////////////////////////
          
            
            int y = pipesPerWrapUnit;
            int z = responseWrapUnits;
            
            Pipe[] singlePipe = new Pipe[z*y];
            Pipe[][] fromSupers = new Pipe[][]{singlePipe};
            
            Pipe[] toWiterPipes = null;
            
            if (isTLS) {
	            
	            toWiterPipes = new Pipe[(z*y) + requestUnwrapUnits ]; //extras for handshakes if needed
	            
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
		            	toWrapperPipes[w] = new Pipe<NetPayloadSchema>(toWraperConfig,false);
		            	fromWrapperPipes[w] = new Pipe<NetPayloadSchema>(fromWraperConfig,false); 
		            	toWiterPipes[toWriterPos++] = fromWrapperPipes[w];
		            	fromSupers[g][fromSuperPos++] = toWrapperPipes[w]; //TODO: this zero is wrong because it should be the count of apps.
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

            	int i = fromSupers[g].length;
            	while (-- i>= 0) {
            		fromSupers[g][i]=new Pipe<NetPayloadSchema>(fromWraperConfig,false);            		
            	}
            	toWiterPipes = fromSupers[g];      	
            
            }
            
            
            ///////////////////
            //we always have a super to ensure order regardless of TLS
            //a single supervisor will group all the modules responses together.
            ///////////////////

            Pipe[][] orderedOutput = Pipe.splitPipes(routerCount, fromSupers[g]);
            int k = routerCount;
            while (--k>=0) {
                        
            	OrderSupervisorStage wrapSuper = new OrderSupervisorStage(graphManager, fromModule[k], orderedOutput[k], coordinator, isTLS);//ensure order           
            }
            
            ///////////////
            //all the writer stages
            ///////////////
            
            
            Pipe[][] req = Pipe.splitPipes(socketWriters, toWiterPipes);	//TODO: for multiple apps this is also not yet ready.
            int w = socketWriters;
            while (--w>=0) {
            	
            	ServerSocketWriterStage writerStage = new ServerSocketWriterStage(graphManager, coordinator, req[w], g); //pump bytes out
                GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketWriter", writerStage);
               	
            }
           
        }
              
        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(newConnectionsConfig,false);        
        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator, newConnectionsPipe, isTLS); 
        PipeCleanerStage<ServerConnectionSchema> dump = new PipeCleanerStage<>(graphManager, newConnectionsPipe); //IS this important data?
        
        return graphManager;
	}

	private static void populateGraphWithUnWrapStages(GraphManager graphManager, ServerCoordinator coordinator,
			int requestUnwrapUnits, PipeConfig<NetPayloadSchema> handshakeDataConfig, Pipe[][] encryptedIncomingGroup,
			Pipe[][] planIncomingGroup, Pipe[][] handshakeIncomingGroup, int g, Pipe[] acks) {
		handshakeIncomingGroup[g] = new Pipe[requestUnwrapUnits];
		            	
		int c = requestUnwrapUnits;
		Pipe[][] in = Pipe.splitPipes(c, encryptedIncomingGroup[g]);
		Pipe[][] out = Pipe.splitPipes(c, planIncomingGroup[g]);
		
		while (--c>=0) {
			handshakeIncomingGroup[g][c] = new Pipe(handshakeDataConfig);
			SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(graphManager, coordinator, in[c], out[c], acks[c], handshakeIncomingGroup[g][c], true, 0);
			GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
		}
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

	public static List<InetAddress> homeAddresses(boolean noIPV6) {
		List<InetAddress> addrList = new ArrayList<InetAddress>();
		try {
			Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();			
			while (networkInterfaces.hasMoreElements()) {
				NetworkInterface ifc = networkInterfaces.nextElement();
				try {
					if(ifc.isUp()) {						
						Enumeration<InetAddress> addrs = ifc.getInetAddresses();
						while (addrs.hasMoreElements()) {
							InetAddress addr = addrs.nextElement();						
							byte[] addrBytes = addr.getAddress();
							if (noIPV6) {								
								if (16 == addrBytes.length) {
									continue;
								}							
							}							
							if (addrBytes.length==4) {
								if (addrBytes[0]==127 && addrBytes[1]==0 && addrBytes[2]==0 && addrBytes[3]==1) {
									continue;
								}								
							}
							addrList.add(addr);
						}						
					}
				} catch (SocketException e) {
					//ignore
				}
			}			
		} catch (SocketException e1) {
			//ignore.
		}
		
		Comparator<? super InetAddress> comp = new Comparator<InetAddress>() {
			@Override
			public int compare(InetAddress o1, InetAddress o2) {
				return Integer.compare(o2.getAddress()[0], o2.getAddress()[0]);
			} //decending addresses			
		};
		addrList.sort(comp);
		return addrList;
	}

	public static ServerCoordinator httpServerSetup(boolean isTLS, String bindHost, int port, GraphManager gm, boolean large, ModuleConfig config) {
		
		int cores = Runtime.getRuntime().availableProcessors();
		if (cores>64) {
			cores = cores >> 2;
		}
		if (cores<4) {
			cores = 4;
		}
		
		logger.info("cores in use {}", cores);
		
		
		final int maxPartialResponsesServer;
		final int maxConnectionBitsOnServer; 	
		final int serverRequestUnwrapUnits;
		final int serverResponseWrapUnits;
		final int serverPipesPerOutputEngine;
		final int serverSocketWriters;
		final int groups;
		final int serverInputBlobs; 
		final int serverBlobToEncrypt;
		final int serverBlobToWrite;
		final int serverInputMsg;
		
		final int serverMsgToEncrypt;
		final int serverOutputMsg;
			
		final int fromRouterMsg;
		final int fromRouterBlob;
		final int routerCount;
		final int releaseMsg;
		
		if (large) {
			
			
			maxPartialResponsesServer     = 32;//256;    // (big memory consumption!!) concurrent partial messages 
			maxConnectionBitsOnServer 	  = 20;       //1M open connections on server	    	
			groups                        = 1;
			
			serverInputMsg                = 20;
			serverInputBlobs              = 1<<14;
			
			serverMsgToEncrypt            = 512;
			serverBlobToEncrypt           = 1<<14;
			
			serverOutputMsg               = isTLS?128:512;
			serverBlobToWrite             = 1<<12;
			
			fromRouterMsg 				  = isTLS?512:2048;//impacts performance
			fromRouterBlob 				  = 1<<10;
			
			serverSocketWriters           = isTLS?1:4;
			routerCount                   = isTLS?2:8;
			releaseMsg                    = 512;
						
			serverRequestUnwrapUnits      = isTLS?4:2;  //server unwrap units - need more for handshaks and more for posts
			serverResponseWrapUnits 	  = isTLS?8:4;    //server wrap units
			serverPipesPerOutputEngine 	  = isTLS?4:8;//multiplier against server wrap units for max simultanus user responses.
			
		} else {	//small
			maxPartialResponsesServer     = 32;    //16 concurrent partial messages 
			maxConnectionBitsOnServer     = 12;    //4k  open connections on server	    	
			groups                        = 1;	
		
			serverInputMsg                = isTLS? 8 : 8; 
			serverInputBlobs              = isTLS? 1<<15 : 1<<8;  
			
			serverMsgToEncrypt            = 128;
			serverBlobToEncrypt           = 1<<15; //Must NOT be smaller than the file write output (modules) ??? ONLY WHEN WE ARE USE ING TLS
			
			serverOutputMsg               = isTLS?32:128; //important for outgoing data and greatly impacts performance
			serverBlobToWrite             = 1<<10; //Must NOT be smaller than the file write output (modules), bigger values support combined writes when tls is off
			
			fromRouterMsg 			      = isTLS?256:2048; //impacts performance
			fromRouterBlob				  = 1<<7;
			
			routerCount                   = 4;

			serverSocketWriters           = 1;
			releaseMsg                    = 256;
						
			serverRequestUnwrapUnits      = isTLS?4:2;  //server unwrap units - need more for handshaks and more for posts
			serverResponseWrapUnits 	  = isTLS?8:4;    //server wrap units
			serverPipesPerOutputEngine 	  = isTLS?4:8;//multiplier against server wrap units for max simultanus user responses.
		}
		

		
		
		 
		 //This must be large enough for both partials and new handshakes.
	
		ServerCoordinator serverCoord = new ServerCoordinator(groups, bindHost, port, maxConnectionBitsOnServer, maxPartialResponsesServer, routerCount);	
		
		
		
		
		buildHTTPServerGraph(isTLS, gm, groups, maxPartialResponsesServer, config, serverCoord,
				                             serverRequestUnwrapUnits, serverResponseWrapUnits, serverPipesPerOutputEngine, serverSocketWriters, 
				                            serverInputMsg, serverInputBlobs, serverMsgToEncrypt, serverBlobToEncrypt, serverOutputMsg,
				                              serverBlobToWrite, routerCount, fromRouterMsg, fromRouterBlob, releaseMsg );
		return serverCoord;
	}
	
}
