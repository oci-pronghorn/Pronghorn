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

import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.http.HTTP1xResponseParserStage;
import com.ociweb.pronghorn.network.http.HTTP1xRouterStage;
import com.ociweb.pronghorn.network.http.HTTP1xRouterStageConfig;
import com.ociweb.pronghorn.network.http.HTTPClientRequestStage;
import com.ociweb.pronghorn.network.http.ModuleConfig;
import com.ociweb.pronghorn.network.module.AbstractPayloadResponseStage;
import com.ociweb.pronghorn.network.module.DotModuleStage;
import com.ociweb.pronghorn.network.module.ResourceModuleStage;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class NetGraphBuilder {
	
	private static final Logger logger = LoggerFactory.getLogger(NetGraphBuilder.class);	
	
	public static void buildHTTPClientGraph(GraphManager gm, int maxPartialResponses,
			ClientCoordinator ccm, final IntHashTable listenerPipeLookup,
			int responseQueue, 
			int responseSize, final Pipe<NetPayloadSchema>[] requests, 
			final Pipe<NetResponseSchema>[] responses) {
		
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {

			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, 
								    Pipe<NetPayloadSchema>[] clearResponse,
								    Pipe<ReleaseSchema> ackReleaseForResponseParser) {
				
				buildHTTP1xResponseParser(gm, ccm, listenerPipeLookup, responses, clearResponse, ackReleaseForResponseParser);
			}
			
		};
		
		buildClientGraph(gm, ccm, responseQueue, responseSize, requests, 2, 2, 
				             2, 2048, 64, 1<<19, factory, 20);
	}
	
	public static void buildClientGraph(GraphManager gm, ClientCoordinator ccm, int responseQueue, int responseSize,
										Pipe<NetPayloadSchema>[] requests, int responseUnwrapCount, int clientWrapperCount,
										int clientWriters, 
										int releaseCount, int netResponseCount, int netResponseBlob, 
										ClientResponseParserFactory parserFactory,
										int writeBufferMultiplier
										) {
	
		int maxPartialResponses = ccm.resposePoolSize();
		
		PipeConfig<ReleaseSchema> parseReleaseConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance, releaseCount, 0);
		

		//must be large enough for handshake plus this is the primary pipe after the socket so it must be a little larger.
		PipeConfig<NetPayloadSchema> clientNetResponseConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, responseQueue, responseSize); 	
		
		
		//pipe holds data as it is parsed so making it larger is helpful
		PipeConfig<NetPayloadSchema> clientHTTPResponseConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, netResponseCount, netResponseBlob); 	
		
		
		///////////////////
		//add the stage under test
		////////////////////

				
		//the responding reading data is encrypted so there is not much to be tested
		//we will test after the unwrap
		//SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(gm, ccm, socketResponse, clearResponse, false, 0);
		
		Pipe<NetPayloadSchema>[] socketResponse;
		Pipe<NetPayloadSchema>[] clearResponse;
		if (ccm.isTLS) {
			//NEED EVEN SPLIT METHOD FOR ARRAY.
			socketResponse = new Pipe[maxPartialResponses];
			clearResponse = new Pipe[maxPartialResponses];		
					
			int k = maxPartialResponses;
			while (--k>=0) {
				socketResponse[k] = new Pipe<NetPayloadSchema>(clientNetResponseConfig, false);
				clearResponse[k] = new Pipe<NetPayloadSchema>(clientHTTPResponseConfig); //may be consumed by high level API one does not know.
			}
		} else {
			socketResponse = new Pipe[maxPartialResponses];
			clearResponse = socketResponse;		
			
			int k = maxPartialResponses;
			while (--k>=0) {
				socketResponse[k] = new Pipe<NetPayloadSchema>(clientHTTPResponseConfig);//may be consumed by high level API one does not know.
			}
		}
			
		final int responseParsers = 1;		
		int a = responseParsers + (ccm.isTLS?responseUnwrapCount:0);
		Pipe<ReleaseSchema>[] acks = new Pipe[a];
		while (--a>=0) {
			acks[a] =  new Pipe<ReleaseSchema>(parseReleaseConfig); //may be consumed by high level API one does not know.	
		}
		Pipe<ReleaseSchema> ackReleaseForResponseParser = acks[acks.length-1];
		
		ClientSocketReaderStage socketReaderStage = new ClientSocketReaderStage(gm, ccm, acks, socketResponse);
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "SocketReader", socketReaderStage);	
		
		Pipe<NetPayloadSchema>[] hanshakePipes = buildClientUnwrap(gm, ccm, requests, responseUnwrapCount, socketResponse, clearResponse,	acks);	

		buildClientWrapAndWrite(gm, ccm, requests, clientWrapperCount, clientWriters, hanshakePipes, writeBufferMultiplier);	    

		parserFactory.buildParser(gm, ccm, clearResponse, ackReleaseForResponseParser);
	    
	}

	private static Pipe<NetPayloadSchema>[] buildClientUnwrap(GraphManager gm, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] requests,
			int responseUnwrapCount, Pipe<NetPayloadSchema>[] socketResponse, Pipe<NetPayloadSchema>[] clearResponse,
			Pipe<ReleaseSchema>[] acks) {
		Pipe<NetPayloadSchema>[] hanshakePipes = null;
		if (ccm.isTLS) {
						
			int c = responseUnwrapCount;
			Pipe<NetPayloadSchema>[][] sr = Pipe.splitPipes(c, socketResponse);
			Pipe<NetPayloadSchema>[][] cr = Pipe.splitPipes(c, clearResponse);
			
			hanshakePipes = new Pipe[c];
			
			while (--c>=0) {
				hanshakePipes[c] = new Pipe<NetPayloadSchema>(requests[0].config(),false); 
				SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(gm, ccm, sr[c], cr[c], acks[c], hanshakePipes[c], false, 0);
				GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
			}
			
		}
		return hanshakePipes;
	}

	private static void buildClientWrapAndWrite(GraphManager gm, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] requests,
			int clientWrapperCount, int clientWriters, Pipe<NetPayloadSchema>[] hanshakePipes, int writeBufferMultiplier) {
		//////////////////////////////
		//////////////////////////////
		Pipe<NetPayloadSchema>[] wrappedClientRequests;		
		if (ccm.isTLS) {
			wrappedClientRequests = new Pipe[requests.length];	
			int j = requests.length;
			while (--j>=0) {								
				wrappedClientRequests[j] = new Pipe<NetPayloadSchema>(requests[j].config(),false);
			}
			
			int c = clientWrapperCount;			
			Pipe<NetPayloadSchema>[][] plainData = Pipe.splitPipes(c, requests);
			Pipe<NetPayloadSchema>[][] encrpData = Pipe.splitPipes(c, wrappedClientRequests);
			while (--c>=0) {			
				if (encrpData[c].length>0) {
					SSLEngineWrapStage wrapStage = new  SSLEngineWrapStage(gm, ccm, false, plainData[c], encrpData[c] );
					GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "Wrap", wrapStage);
				}
			}
			
			//change order of pipes for split later
			//interleave the handshakes.
			c = hanshakePipes.length;
			Pipe<NetPayloadSchema>[][] tPipes = Pipe.splitPipes(c, wrappedClientRequests);
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
				
		Pipe<NetPayloadSchema>[][] clientRequests = Pipe.splitPipes(clientWriters, wrappedClientRequests);
		
		int i = clientWriters;
		
		while (--i>=0) {		
			ClientSocketWriterStage socketWriteStage = new ClientSocketWriterStage(gm, ccm, writeBufferMultiplier, clientRequests[i]);
	    	GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "SocketWriter", socketWriteStage);
	    	//GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 100_000_000, socketWriteStage);//slow down writers.
	    	
		}
	}

	public static void buildHTTP1xResponseParser(GraphManager gm, ClientCoordinator ccm, IntHashTable listenerPipeLookup,
			Pipe<NetResponseSchema>[] responses, Pipe<NetPayloadSchema>[] clearResponse,
			Pipe<ReleaseSchema> ackRelease) {
		HTTP1xResponseParserStage parser = new HTTP1xResponseParserStage(gm, clearResponse, responses, ackRelease, listenerPipeLookup, ccm, HTTPSpecification.defaultSpec());
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "HTTPParser", parser);
	}

	private static void buildParser(GraphManager gm, ClientCoordinator ccm, IntHashTable listenerPipeLookup,
			Pipe<NetResponseSchema>[] responses, Pipe<NetPayloadSchema>[] clearResponse, Pipe<ReleaseSchema>[] acks) {
		
		HTTP1xResponseParserStage parser = new HTTP1xResponseParserStage(gm, clearResponse, responses, acks[acks.length-1], listenerPipeLookup, ccm, HTTPSpecification.defaultSpec());
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "HTTPParser", parser);
	}


	public static GraphManager buildHTTPServerGraph(final GraphManager graphManager, final ModuleConfig modules, final ServerCoordinator coordinator,
			                                        final ServerPipesConfig serverConfig) {

        return buildServerGraph(graphManager, coordinator, serverConfig, -1,
        		(gm,coord,release,from,to) -> {
        			buildHTTPStages(gm, coord, modules, serverConfig, release, from, to, -1); //-1, do not adjust rate
        });
        
	}

	public static GraphManager buildSimpleServerGraph(final GraphManager graphManager,
			final ServerCoordinator coordinator, 
			boolean isLarge, boolean isTLS,
			ServerFactory factory) {
		
		return buildServerGraph(graphManager, coordinator,
				                new ServerPipesConfig(isLarge,isTLS),-1,factory);
		
	}
	
	public static GraphManager buildServerGraph(final GraphManager graphManager,
													final ServerCoordinator coordinator, 
													final ServerPipesConfig serverConfig, 
													final long rate, 
													ServerFactory factory) {
		
		final Pipe<NetPayloadSchema>[] encryptedIncomingGroup = Pipe.buildPipes(serverConfig.maxPartialResponsesServer, serverConfig.incomingDataConfig);           
           
        Pipe<ReleaseSchema>[] releaseAfterParse = buildSocketReaderStage(graphManager, coordinator, coordinator.processorCount(),
        		                                                         serverConfig, encryptedIncomingGroup, rate);
                       
        Pipe<NetPayloadSchema>[] handshakeIncomingGroup=null;
        Pipe<NetPayloadSchema>[] receivedFromNet;
        
        if (coordinator.isTLS) {
        	receivedFromNet = Pipe.buildPipes(serverConfig.maxPartialResponsesServer, serverConfig.incomingDataConfig);
        	handshakeIncomingGroup = populateGraphWithUnWrapStages(graphManager, coordinator, serverConfig.serverRequestUnwrapUnits, serverConfig.handshakeDataConfig,
        			                      encryptedIncomingGroup, receivedFromNet, releaseAfterParse, rate);
        } else {
        	receivedFromNet = encryptedIncomingGroup;
        }

        Pipe<NetPayloadSchema>[] sendingToNet = buildRemainderOfServerStages(graphManager, coordinator,
									        		serverConfig, handshakeIncomingGroup, rate);
        
        factory.buildServer(graphManager, coordinator, 
        		            releaseAfterParse, receivedFromNet, sendingToNet);

        return graphManager;
	}

	private static void buildHTTPStages(GraphManager graphManager, ServerCoordinator coordinator, ModuleConfig modules,
			ServerPipesConfig serverConfig, Pipe<ReleaseSchema>[] releaseAfterParse,
			Pipe<NetPayloadSchema>[] receivedFromNet, Pipe<NetPayloadSchema>[] sendingToNet, long rate) {

		HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> httpSpec = HTTPSpecification.defaultSpec();
		
		if (modules.moduleCount()==0) {
			throw new UnsupportedOperationException("Must be using at least 1 module to startup.");
		}
		
		int routerCount = coordinator.processorCount();

		
        Pipe<ServerResponseSchema>[][] fromModule = new Pipe[routerCount][];         
        Pipe<HTTPRequestSchema>[][] toModules = new Pipe[routerCount][];
        
        PipeConfig<HTTPRequestSchema> routerToModuleConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, serverConfig.fromProcessorCount, serverConfig.fromProcessorBlob);///if payload is smaller than average file size will be slower
        final HTTP1xRouterStageConfig routerConfig = buildModules(graphManager, modules, routerCount, httpSpec, routerToModuleConfig, fromModule, toModules);
        
        PipeConfig<ServerResponseSchema> config = ServerResponseSchema.instance.newPipeConfig(4, 512);
        Pipe<ServerResponseSchema>[] errorResponsePipes = buildErrorResponsePipes(routerCount, fromModule, config);        
        
        buildRouters(graphManager, routerCount, receivedFromNet, 
        		     releaseAfterParse, toModules, errorResponsePipes, routerConfig, coordinator, rate);
		        
        buildOrderingSupers(graphManager, coordinator, routerCount, 
        		            fromModule, sendingToNet, rate);
	}

	private static Pipe<ServerResponseSchema>[] buildErrorResponsePipes(final int routerCount,
			Pipe<ServerResponseSchema>[][] fromModule, PipeConfig<ServerResponseSchema> config) {
		Pipe<ServerResponseSchema>[] errorResponsePipes = new Pipe[routerCount];
        int r = routerCount;
        while (--r>=0) {
        	errorResponsePipes[r] = new Pipe<ServerResponseSchema>(config);        	
        	fromModule[r] = PronghornStage.join(fromModule[r],errorResponsePipes[r]);
        }
		return errorResponsePipes;
	}

	public static Pipe<ReleaseSchema>[] buildSocketReaderStage(GraphManager graphManager, ServerCoordinator coordinator, final int routerCount,
			ServerPipesConfig serverConfig, Pipe<NetPayloadSchema>[] encryptedIncomingGroup, long rate) {
		int a = routerCount+(coordinator.isTLS?serverConfig.serverRequestUnwrapUnits:0);
		Pipe<ReleaseSchema>[] acks = new Pipe[a];
		while (--a>=0) {
			acks[a] =  new Pipe<ReleaseSchema>(serverConfig.releaseConfig, false);	
		}
                   
        //reads from the socket connection
        ServerSocketReaderStage readerStage = new ServerSocketReaderStage(graphManager, acks, encryptedIncomingGroup, coordinator);
        GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketReader", readerStage);
        if (rate>0) {
        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, readerStage);
        }
		return acks;
	}

	public static Pipe<NetPayloadSchema>[] buildRemainderOfServerStages(final GraphManager graphManager,
			ServerCoordinator coordinator,
			ServerPipesConfig serverConfig, 
			Pipe<NetPayloadSchema>[] handshakeIncomingGroup, long rate) {
		Pipe<NetPayloadSchema>[] fromOrderedContent;
		{
		fromOrderedContent = new Pipe[serverConfig.serverResponseWrapUnits * serverConfig.serverPipesPerOutputEngine];
        
        Pipe<NetPayloadSchema>[] toWiterPipes = buildSSLWrapersAsNeeded(graphManager, coordinator, serverConfig.serverRequestUnwrapUnits, serverConfig.toWraperConfig,
        												serverConfig.fromWraperConfig, handshakeIncomingGroup, 
        												serverConfig.serverPipesPerOutputEngine, serverConfig.serverResponseWrapUnits,
        												fromOrderedContent, rate);
                    
        buildSocketWriters(graphManager, coordinator, serverConfig.serverSocketWriters, toWiterPipes, 
        		           serverConfig.writeBufferMultiplier, rate);

              
        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(serverConfig.newConnectionsConfig,false);        
        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator, newConnectionsPipe); 
        if (rate>0) {
        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, newConStage);
        }
        PipeCleanerStage<ServerConnectionSchema> dump = new PipeCleanerStage<>(graphManager, newConnectionsPipe); //IS this important data?
        if (rate>0) {
        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, dump);
        }
		}
		return fromOrderedContent;
	}

	private static Pipe<NetPayloadSchema>[] buildSSLWrapersAsNeeded(GraphManager graphManager, ServerCoordinator coordinator,
			int requestUnwrapUnits, PipeConfig<NetPayloadSchema> toWraperConfig, PipeConfig<NetPayloadSchema> fromWraperConfig,
			Pipe<NetPayloadSchema>[] handshakeIncomingGroup, int y, int z,
			Pipe<NetPayloadSchema>[] fromOrderedContent, long rate) {
		
		Pipe<NetPayloadSchema>[] toWiterPipes = null;
		
		if (coordinator.isTLS) {
		    
		    toWiterPipes = new Pipe[(z*y) + requestUnwrapUnits ]; //extras for handshakes if needed
		    
		    int toWriterPos = 0;
		    int fromSuperPos = 0;
		    
		    int remHanshakePipes = requestUnwrapUnits;
		    
		    while (--z>=0) {           
		    	
		    	//as possible we must mix up the pipes to ensure handshakes go to different writers.
		        if (--remHanshakePipes>=0) {
		        	toWiterPipes[toWriterPos++] = handshakeIncomingGroup[remHanshakePipes]; //handshakes go directly to the socketWriterStage
		        }
		    	
		    	//
		    	int w = y;
		        Pipe<NetPayloadSchema>[] toWrapperPipes = new Pipe[w];
		        Pipe<NetPayloadSchema>[] fromWrapperPipes = new Pipe[w];            
		        
		        while (--w>=0) {	
		        	toWrapperPipes[w] = new Pipe<NetPayloadSchema>(toWraperConfig,false);
		        	fromWrapperPipes[w] = new Pipe<NetPayloadSchema>(fromWraperConfig,false); 
		        	toWiterPipes[toWriterPos++] = fromWrapperPipes[w];
		        	fromOrderedContent[fromSuperPos++] = toWrapperPipes[w]; 
		        }
		        
		        boolean isServer = false; //TODO: this should be true??
		        
				SSLEngineWrapStage wrapStage = new SSLEngineWrapStage(graphManager, coordinator,
		        		                                             isServer, toWrapperPipes, fromWrapperPipes);
		        GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "Wrap", wrapStage);
		        if (rate>0) {
		        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, wrapStage);
		        }
		    }
		    
		    //finish up any remaining handshakes
		    while (--remHanshakePipes>=0) {
		    	toWiterPipes[toWriterPos++] = handshakeIncomingGroup[remHanshakePipes]; //handshakes go directly to the socketWriterStage
		    }
		    
		    
		} else {

			int i = fromOrderedContent.length;
			while (-- i>= 0) {
				fromOrderedContent[i] = new Pipe<NetPayloadSchema>(fromWraperConfig,false);            		
			}
			toWiterPipes = fromOrderedContent;      	
		
		}
		return toWiterPipes;
	}

	public static void buildOrderingSupers(GraphManager graphManager, 
			                               ServerCoordinator coordinator, final int routerCount,
			                               Pipe<ServerResponseSchema>[][] fromModule, 
			                               Pipe<NetPayloadSchema>[] fromSupers, long rate) {
		///////////////////
		//we always have a super to ensure order regardless of TLS
		//a single supervisor will group all the modules responses together.
		///////////////////

		Pipe<NetPayloadSchema>[][] orderedOutput = Pipe.splitPipes(routerCount, fromSupers);
		int k = routerCount;
		while (--k>=0) {
			OrderSupervisorStage wrapSuper = new OrderSupervisorStage(graphManager, fromModule[k], orderedOutput[k], coordinator);//ensure order           
			if (rate>0) {
				GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, wrapSuper);
			}
		
		}
	}

	private static void buildSocketWriters(GraphManager graphManager, ServerCoordinator coordinator, 
											int socketWriters, Pipe<NetPayloadSchema>[] toWiterPipes, 
											int writeBufferMultiplier, long rate) {
		///////////////
		//all the writer stages
		///////////////
		
		
		Pipe[][] req = Pipe.splitPipes(socketWriters, toWiterPipes);	
		int w = socketWriters;
		while (--w>=0) {
			
			ServerSocketWriterStage writerStage = new ServerSocketWriterStage(graphManager, coordinator, writeBufferMultiplier, req[w]); //pump bytes out
		    GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketWriter", writerStage);
		    if (rate>0) {
	        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, writerStage);
	        }
		   	
		}
	}

	public static void buildRouters(GraphManager graphManager, final int routerCount, Pipe[] planIncomingGroup,
									Pipe[] acks, 
									Pipe<HTTPRequestSchema>[][] toModules, 
									Pipe<ServerResponseSchema>[] errorResponsePipes,
									final HTTP1xRouterStageConfig routerConfig, 
									ServerCoordinator coordinator, long rate) {
		
		int a;
		/////////////////////
		//create the routers
		/////////////////////
		//split up the unencrypted pipes across all the routers
		Pipe[][] plainSplit = Pipe.splitPipes(routerCount, planIncomingGroup);
		int acksBase = acks.length-1;
		int r = routerCount;
		while (--r>=0) {
			
			HTTP1xRouterStage router = HTTP1xRouterStage.newInstance(graphManager, plainSplit[r], 
															toModules[r], errorResponsePipes[r], acks[acksBase-r], routerConfig, coordinator);        
			GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "HTTPParser", router);
			if (rate>0) {
				GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, router);
			}
			
		}
		
		
	}

	public static HTTP1xRouterStageConfig buildModules(GraphManager graphManager, ModuleConfig modules,
			final int routerCount,
			HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> httpSpec,
			PipeConfig<HTTPRequestSchema> routerToModuleConfig, Pipe<ServerResponseSchema>[][] fromModule,
			Pipe<HTTPRequestSchema>[][] toModules) {
		
		final HTTP1xRouterStageConfig routerConfig = new HTTP1xRouterStageConfig(httpSpec); 
		//create the modules

		for(int r=0; r<routerCount; r++) {
			toModules[r] = new Pipe[modules.moduleCount()];
		}
		  
		//create each module
		for(int a=0; a<modules.moduleCount(); a++) { 
			
			Pipe[] routesTemp = new Pipe[routerCount];
			for(int r=0; r<routerCount; r++) {
				//TODO: this should be false but the DOT telemetry is still using the high level API...
				routesTemp[r] = toModules[r][a] =  new Pipe<HTTPRequestSchema>(routerToModuleConfig);//,false);				
			}
			//each module can unify of split across routers
			routerConfig.registerRoute(modules.getPathRoute(a),
					                   modules.addModule(a, graphManager, routesTemp, httpSpec),
					                   httpSpec.headerParser());
		    
			//one array per each of the routers.
		    Pipe<ServerResponseSchema>[][] outputPipes = modules.outputPipes(a);
		  
		    for(int r=0; r<routerCount; r++) {
		    	fromModule[r] = PronghornStage.join(outputPipes[r], fromModule[r]);
		    }
		    
		}
		
		
		return routerConfig;
	}

	public static Pipe<NetPayloadSchema>[] populateGraphWithUnWrapStages(GraphManager graphManager, ServerCoordinator coordinator,
			int requestUnwrapUnits, PipeConfig<NetPayloadSchema> handshakeDataConfig, Pipe[] encryptedIncomingGroup,
			Pipe[] planIncomingGroup, Pipe[] acks, long rate) {
		Pipe<NetPayloadSchema>[] handshakeIncomingGroup = new Pipe[requestUnwrapUnits];
		            	
		int c = requestUnwrapUnits;
		Pipe[][] in = Pipe.splitPipes(c, encryptedIncomingGroup);
		Pipe[][] out = Pipe.splitPipes(c, planIncomingGroup);
		
		while (--c>=0) {
			handshakeIncomingGroup[c] = new Pipe(handshakeDataConfig);
			SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(graphManager, coordinator, in[c], out[c], acks[c], handshakeIncomingGroup[c], true, 0);
			GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
			 if (rate>0) {
		        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, unwrapStage);
		     }
		}
		
		return handshakeIncomingGroup;
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
	

	public static ServerCoordinator httpServerSetup(boolean isTLS, String bindHost, int port, GraphManager gm, boolean large, ModuleConfig modules) {
		
		ServerPipesConfig serverConfig = new ServerPipesConfig(large, isTLS);
				 
		 //This must be large enough for both partials and new handshakes.
	
		ServerCoordinator serverCoord = new ServerCoordinator(isTLS, bindHost, port, serverConfig.maxConnectionBitsOnServer, serverConfig.maxPartialResponsesServer, serverConfig.processorCount);
		
		buildHTTPServerGraph(gm, modules, serverCoord, serverConfig);
		
		return serverCoord;
	}
	
	public static void telemetryServerSetup(boolean isTLS, String bindHost, int port, GraphManager gm) {
	
		final long rate = 20_000_000; //fastest rate in NS
		
		boolean isLarge = false;
		ModuleConfig config = new ModuleConfig(){

			private final int moduleCount = 3;
			private Pipe<ServerResponseSchema>[][] staticFileOutputs;
			
			
			@Override
			public IntHashTable addModule(int a, 
					GraphManager graphManager, 
					Pipe<HTTPRequestSchema>[] inputs,
					HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> spec) {
		
					//the file server is stateless therefore we can build 1 instance for every input pipe
					int instances = inputs.length;
					
					if (null == staticFileOutputs) {
						staticFileOutputs = new Pipe[instances][moduleCount];
					}
					
					int i = instances;
					while (--i>=0) {
						
						switch (a) {
							case 0:
							GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate,
									ResourceModuleStage.newInstance(graphManager, 
											inputs[i], 
											staticFileOutputs[i][0] = ServerResponseSchema.instance.newPipe(4, 1<<15), 
											spec,
											"telemetry/index.html", HTTPContentTypeDefaults.HTML)
									);
							break;
							case 1:
								GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate,
								ResourceModuleStage.newInstance(graphManager, 
						                  inputs[i], 
						                  staticFileOutputs[i][1] = ServerResponseSchema.instance.newPipe(4, 1<<15), 
						                  spec,
						                  "telemetry/viz-lite.js", HTTPContentTypeDefaults.JS)
								);
							break;
							case 2:
								GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, 
										DotModuleStage.newInstance(graphManager, 
												inputs[i], 
												staticFileOutputs[i][2] = ServerResponseSchema.instance.newPipe(4, 1<<15), 
												spec)
										);
								break;
							default:
								throw new RuntimeException("unknonw idx "+a);
						}
						
					}
				return IntHashTable.EMPTY;
			}
		
			@Override
			public CharSequence getPathRoute(int a) {
				switch(a) {
					case 0:
						return "/";
					case 1:
						return "/viz-lite.js";
					case 2:
						return "/graph.dot";
					default:
						throw new RuntimeException("unknown value "+a);
				}
			}
		
			@Override
			public Pipe<ServerResponseSchema>[][] outputPipes(int a) {
				
				int i = staticFileOutputs.length;
				Pipe<ServerResponseSchema>[][] temp = new Pipe[i][1];
				while (--i>=0) {
					temp[i][0] = staticFileOutputs[i][a];
				}
				return temp;
			}
		
			@Override
			public int moduleCount() {
				return moduleCount;
			}  
			
		
		};
		ServerPipesConfig serverConfig = new ServerPipesConfig(isLarge, isTLS, 1);
				 
		 //This must be large enough for both partials and new handshakes.
		
		ServerCoordinator serverCoord = new ServerCoordinator(isTLS, bindHost, port, serverConfig.maxConnectionBitsOnServer, serverConfig.maxPartialResponsesServer, serverConfig.processorCount);
		final ModuleConfig modules = config;
		final ServerCoordinator coordinator = serverCoord;
		
		NetGraphBuilder.buildServerGraph(gm, coordinator, serverConfig, rate, 
				(gm1,coord,release,from,to) -> {
					NetGraphBuilder.buildHTTPStages(gm1, coord, modules, serverConfig, release, from, to, rate);
		});
			 
	}

	/**
	 * Build HTTP client subgraph.  This is the easiest method to set up the client calls since many default values are already set.
	 * 
	 * @param gm target graph where this will be added
	 * @param httpResponsePipe http responses 
	 * @param httpRequestsPipe http requests
	 */	
	public static void buildHTTPClientGraph(GraphManager gm,
			  int maxPartialResponses,
			  Pipe<NetResponseSchema>[] httpResponsePipe,
			  Pipe<ClientHTTPRequestSchema>[] httpRequestsPipe) {		
		
		int connectionsInBits = 6;		
		int clientRequestCount = 4;
		int clientRequestSize = 1<<15;
		boolean isTLS = true;
		
		buildHTTPClientGraph(gm, maxPartialResponses, httpResponsePipe, httpRequestsPipe, connectionsInBits,
								clientRequestCount, clientRequestSize, isTLS);
		
		
	}

	public static void buildHTTPClientGraph(GraphManager gm, int maxPartialResponses,
			Pipe<NetResponseSchema>[] httpResponsePipe, Pipe<ClientHTTPRequestSchema>[] httpRequestsPipe,
			int connectionsInBits, int clientRequestCount, int clientRequestSize, boolean isTLS) {
		buildHTTPClientGraph(gm, null, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
							 clientRequestCount, clientRequestSize, isTLS);
	}
	
	/**
	 * Build HTTP client subgraph. 
	 * 
	 * @param gm target graph where this will be added
	 * @param netPipeLookup table to map the listener id with the target response pipes, can be null for 1 to 1 mapping
	 * @param httpResponsePipe http responses 
	 * @param httpRequestsPipe http requests
	 */	
	public static void buildHTTPClientGraph(GraphManager gm,
									  IntHashTable netPipeLookup, 
									  final Pipe<NetResponseSchema>[] httpResponsePipe,
									  Pipe<ClientHTTPRequestSchema>[] httpRequestsPipe) {
		
		int maxPartialResponses = IntHashTable.count(netPipeLookup);
		int connectionsInBits=6;		
		int clientRequestCount = 4;
		int clientRequestSize = 1<<15;
		boolean isTLS = true;
		
		buildHTTPClientGraph(gm, netPipeLookup, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
							 clientRequestCount, clientRequestSize, isTLS);
	}

	public static void buildSimpleClientGraph(GraphManager gm, ClientCoordinator ccm,
											  ClientResponseParserFactory factory, 
											  Pipe<NetPayloadSchema>[] clientRequests) {
		int clientWriters = 1;				
		int responseUnwrapCount = 1;
		int clientWrapperCount = 1;
		int responseQueue = 10;
		int responseSize = 1<<17;
		int releaseCount = 2048;
		int netResponseCount = 64;
		int netResponseBlob = 1<<19;
		int writeBufferMultiplier = 20;
				
		buildClientGraph(gm, ccm, responseQueue, responseSize, clientRequests, responseUnwrapCount, clientWrapperCount,
				         clientWriters, releaseCount, netResponseCount, netResponseBlob, factory, writeBufferMultiplier);
	}
	
	public static void buildHTTPClientGraph(GraphManager gm, final IntHashTable netPipeLookup,
			final Pipe<NetResponseSchema>[] httpResponsePipe, Pipe<ClientHTTPRequestSchema>[] requestsPipe,
			int maxPartialResponses, int connectionsInBits, int clientRequestCount, int clientRequestSize,
			boolean isTLS) {
		
		ClientCoordinator ccm = new ClientCoordinator(connectionsInBits, maxPartialResponses, isTLS);
				
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {

			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, 
								    Pipe<NetPayloadSchema>[] clearResponse,
								    Pipe<ReleaseSchema> ackReleaseForResponseParser) {
				
				NetGraphBuilder.buildHTTP1xResponseParser(gm, ccm, netPipeLookup, httpResponsePipe, clearResponse, ackReleaseForResponseParser);
			}			
		};

		Pipe<NetPayloadSchema>[] clientRequests = Pipe.buildPipes(requestsPipe.length, NetPayloadSchema.instance.<NetPayloadSchema>newPipeConfig(clientRequestCount,clientRequestSize));
				
		buildSimpleClientGraph(gm, ccm, factory, clientRequests);
		
		new HTTPClientRequestStage(gm, ccm, requestsPipe, clientRequests);
	}

	
}
