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
import com.ociweb.pronghorn.network.http.RouterStageConfig;
import com.ociweb.pronghorn.network.module.DotModuleStage;
import com.ociweb.pronghorn.network.module.ResourceModuleStage;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.network.schema.TwitterEventSchema;
import com.ociweb.pronghorn.network.schema.TwitterStreamControlSchema;
import com.ociweb.pronghorn.network.twitter.RequestTwitterQueryStreamStage;
import com.ociweb.pronghorn.network.twitter.RequestTwitterUserStreamStage;
import com.ociweb.pronghorn.network.twitter.TwitterJSONToTwitterEventsStage;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class NetGraphBuilder {
	
	private static final Logger logger = LoggerFactory.getLogger(NetGraphBuilder.class);	
	
	public static void buildHTTPClientGraph(GraphManager gm, int maxPartialResponses,
			ClientCoordinator ccm, 
			int responseQueue, 
			int responseSize, final Pipe<NetPayloadSchema>[] requests, 
			final Pipe<NetResponseSchema>[] responses) {
		
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {

			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, 
								    Pipe<NetPayloadSchema>[] clearResponse,
								    Pipe<ReleaseSchema> ackReleaseForResponseParser) {
				
				buildHTTP1xResponseParser(gm, ccm, responses, clearResponse, ackReleaseForResponseParser);
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
		PipeConfig<NetPayloadSchema> clientNetResponseConfig = new PipeConfig<NetPayloadSchema>(
				NetPayloadSchema.instance, responseQueue, responseSize); 	
		
		
		//pipe holds data as it is parsed so making it larger is helpful
		PipeConfig<NetPayloadSchema> clientHTTPResponseConfig = new PipeConfig<NetPayloadSchema>(
				NetPayloadSchema.instance, netResponseCount, netResponseBlob); 	
		
		
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
		ccm.processNota(gm, socketReaderStage);
		
		
		Pipe<NetPayloadSchema>[] hanshakePipes = buildClientUnwrap(gm, ccm, requests, responseUnwrapCount, socketResponse, clearResponse, acks);	

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
			if (clientRequests[i].length>0) {
				ClientSocketWriterStage socketWriteStage = new ClientSocketWriterStage(gm, ccm, writeBufferMultiplier, clientRequests[i]);
		    	GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "SocketWriter", socketWriteStage);
		    	ccm.processNota(gm, socketWriteStage);
			}
		}
	}

	public static void buildHTTP1xResponseParser(GraphManager gm, ClientCoordinator ccm, 
			Pipe<NetResponseSchema>[] responses, Pipe<NetPayloadSchema>[] clearResponse,
			Pipe<ReleaseSchema> ackRelease) {
		
		HTTP1xResponseParserStage parser = new HTTP1xResponseParserStage(gm, clearResponse, responses, ackRelease, ccm, HTTPSpecification.defaultSpec());
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "HTTPParser", parser);
		ccm.processNota(gm, parser);
		
	}

	private static void buildParser(GraphManager gm, ClientCoordinator ccm, 
			Pipe<NetResponseSchema>[] responses, Pipe<NetPayloadSchema>[] clearResponse, Pipe<ReleaseSchema>[] acks) {
		
		HTTP1xResponseParserStage parser = new HTTP1xResponseParserStage(gm, clearResponse, responses, acks[acks.length-1], ccm, HTTPSpecification.defaultSpec());
		GraphManager.addNota(gm, GraphManager.DOT_RANK_NAME, "HTTPParser", parser);
		ccm.processNota(gm, parser);
	}


	public static GraphManager buildHTTPServerGraph(final GraphManager graphManager, final ModuleConfig modules, final ServerCoordinator coordinator,
			                                        final ServerPipesConfig serverConfig) {

		final ServerFactory factory = new ServerFactory() {

			@Override
			public void buildServer(GraphManager gm, ServerCoordinator coordinator,
					Pipe<ReleaseSchema>[] releaseAfterParse, Pipe<NetPayloadSchema>[] receivedFromNet,
					Pipe<NetPayloadSchema>[] sendingToNet) {
				
				buildHTTPStages(gm, coordinator, modules, serverConfig, releaseAfterParse, receivedFromNet, sendingToNet, -1); //-1, do not adjust rate
			}
			
		};
				
        return buildServerGraph(graphManager, coordinator, serverConfig, -1, factory);
        
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
		logger.info("building server graph");
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
		
        Pipe<NetPayloadSchema>[] fromOrderedContent = buildRemainderOFServerStages(graphManager, coordinator, serverConfig, handshakeIncomingGroup, rate);
        
        factory.buildServer(graphManager, coordinator, 
        		            releaseAfterParse,
        		            receivedFromNet, 
        		            fromOrderedContent);

        return graphManager;
	}

	private static void buildHTTPStages(GraphManager graphManager, ServerCoordinator coordinator, ModuleConfig modules,
			ServerPipesConfig serverConfig, Pipe<ReleaseSchema>[] releaseAfterParse,
			Pipe<NetPayloadSchema>[] receivedFromNet, Pipe<NetPayloadSchema>[] sendingToNet, long rate) {

		//logger.info("build http stages");
		HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> httpSpec = HTTPSpecification.defaultSpec();
		
		if (modules.moduleCount()==0) {
			throw new UnsupportedOperationException("Must be using at least 1 module to startup.");
		}
		
		int routerCount = coordinator.processorCount();

		//logger.info("build modules");
        Pipe<ServerResponseSchema>[][] fromModule = new Pipe[routerCount][];       
        Pipe<HTTPRequestSchema>[][] toModules = new Pipe[routerCount][];
        
        PipeConfig<HTTPRequestSchema> routerToModuleConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, serverConfig.fromProcessorCount, serverConfig.fromProcessorBlob);///if payload is smaller than average file size will be slower
        final HTTP1xRouterStageConfig routerConfig = buildModules(graphManager, modules, routerCount, httpSpec, routerToModuleConfig, fromModule, toModules);
        
        //logger.info("build http stages 3");
        PipeConfig<ServerResponseSchema> config = ServerResponseSchema.instance.newPipeConfig(4, 512);
        Pipe<ServerResponseSchema>[] errorResponsePipes = buildErrorResponsePipes(routerCount, fromModule, config);        
        boolean captureAll = false;
        buildRouters(graphManager, routerCount, receivedFromNet, 
        		     releaseAfterParse, toModules, errorResponsePipes, routerConfig, coordinator, rate, captureAll);
		        
        //logger.info("build http ordering supervisors");
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
        coordinator.processNota(graphManager, readerStage);
		return acks;
	}

	public static Pipe<NetPayloadSchema>[] buildRemainderOFServerStages(final GraphManager graphManager,
			ServerCoordinator coordinator, ServerPipesConfig serverConfig,
			Pipe<NetPayloadSchema>[] handshakeIncomingGroup, long rate) {
		
	//	logger.info("build remainder of server");
		PipeConfig<NetPayloadSchema> fromOrderedConfig = serverConfig.orderWrapConfig();
		Pipe<NetPayloadSchema>[] fromOrderedContent = new Pipe[serverConfig.serverResponseWrapUnits * serverConfig.serverPipesPerOutputEngine];

		Pipe<NetPayloadSchema>[] toWiterPipes = buildSSLWrapersAsNeeded(graphManager, coordinator, serverConfig, 
				                                                       handshakeIncomingGroup, rate, 
				                                                       fromOrderedContent, fromOrderedConfig);
                    
        buildSocketWriters(graphManager, coordinator, serverConfig.serverSocketWriters, toWiterPipes, 
        		           serverConfig.writeBufferMultiplier, rate);

        //logger.info("process nota values");
              
        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(serverConfig.newConnectionsConfig,false);        
        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator, newConnectionsPipe); 
        if (rate>0) {
        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, newConStage);
        }
        coordinator.processNota(graphManager, newConStage);
        PipeCleanerStage<ServerConnectionSchema> dump = new PipeCleanerStage<>(graphManager, newConnectionsPipe); //IS this important data?
        if (rate>0) {
        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, dump);
        }
        coordinator.processNota(graphManager, dump);
		return fromOrderedContent;
	}

	private static Pipe<NetPayloadSchema>[] buildSSLWrapersAsNeeded(final GraphManager graphManager,
			ServerCoordinator coordinator, ServerPipesConfig serverConfig,
			Pipe<NetPayloadSchema>[] handshakeIncomingGroup, long rate,
			Pipe<NetPayloadSchema>[] fromOrderedContent, PipeConfig<NetPayloadSchema> fromOrderedConfig) {
		
		
		int requestUnwrapUnits = serverConfig.serverRequestUnwrapUnits;		
		int y = serverConfig.serverPipesPerOutputEngine;
		int z = serverConfig.serverResponseWrapUnits;
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
		        	toWrapperPipes[w] = new Pipe<NetPayloadSchema>(fromOrderedConfig,false);
		        	fromWrapperPipes[w] = new Pipe<NetPayloadSchema>(fromOrderedConfig,false); 
		        	toWiterPipes[toWriterPos++] = fromWrapperPipes[w];
		        	fromOrderedContent[fromSuperPos++] = toWrapperPipes[w]; 
		        }
		        
		        boolean isServer = true;
		        
				SSLEngineWrapStage wrapStage = new SSLEngineWrapStage(graphManager, coordinator,
		        		                                             isServer, toWrapperPipes, fromWrapperPipes);
		        GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "Wrap", wrapStage);
		        if (rate>0) {
		        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, wrapStage);
		        }
		        coordinator.processNota(graphManager, wrapStage);
		    }
		    
		    //finish up any remaining handshakes
		    while (--remHanshakePipes>=0) {
		    	toWiterPipes[toWriterPos++] = handshakeIncomingGroup[remHanshakePipes]; //handshakes go directly to the socketWriterStage
		    }
		    
		    
		} else {
		
			int i = fromOrderedContent.length;
			while (-- i>= 0) {
				fromOrderedContent[i] = new Pipe<NetPayloadSchema>(fromOrderedConfig,false);            		
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
		//logger.info("build ordering supervisors");
		assert(fromSupers.length >= routerCount) : "reduce router count since we only have "+fromSupers.length+" pipes";
		assert(routerCount>0);
		
		Pipe<NetPayloadSchema>[][] orderedOutput = Pipe.splitPipes(routerCount, fromSupers);
		int k = routerCount;
		while (--k>=0) {
						
			OrderSupervisorStage wrapSuper = new OrderSupervisorStage(graphManager, 
					                    fromModule[k], orderedOutput[k], coordinator);//ensure order   
			
			if (rate>0) {
				GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, wrapSuper);
			}
			coordinator.processNota(graphManager, wrapSuper);
		
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
		   	coordinator.processNota(graphManager, writerStage);
		}
	}

	public static void buildRouters(GraphManager graphManager, final int parallelRoutersCount, Pipe[] planIncomingGroup,
									Pipe[] acks, 
									Pipe<HTTPRequestSchema>[][] toModules, 
									Pipe<ServerResponseSchema>[] errorResponsePipes,
									final HTTP1xRouterStageConfig routerConfig, 
									ServerCoordinator coordinator, long rate, 
									boolean catchAll) {

		
		int a;
		/////////////////////
		//create the routers
		/////////////////////
		//split up the unencrypted pipes across all the routers
		Pipe[][] plainSplit = Pipe.splitPipes(parallelRoutersCount, planIncomingGroup);
		int acksBase = acks.length-1;
		int r = parallelRoutersCount;
		while (--r>=0) {
			
			HTTP1xRouterStage router = HTTP1xRouterStage.newInstance(graphManager, r, plainSplit[r], 
					toModules[r], 
					errorResponsePipes[r], 
					acks[acksBase-r], routerConfig,
					coordinator,catchAll);        

			
			GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "HTTPParser", router);
			if (rate>0) {
				GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, router);
			}
			coordinator.processNota(graphManager, router);
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
		for(int moduleInstance=0; moduleInstance<modules.moduleCount(); moduleInstance++) { 
			
			Pipe<HTTPRequestSchema>[] routesTemp = new Pipe[routerCount];
			for(int r=0; r<routerCount; r++) {
				//TODO: change to use.. newHTTPRequestPipe
				//TODO: this should be false but the DOT telemetry is still using the high level API...
				routesTemp[r] = toModules[r][moduleInstance] =  new Pipe<HTTPRequestSchema>(routerToModuleConfig);//,false);
				
				
			}
			//each module can unify of split across routers
			Pipe<ServerResponseSchema>[] outputPipes = modules.registerModule(moduleInstance, graphManager, routerConfig, routesTemp);
			
			assert(validateNoNulls(outputPipes));
		    
		    for(int r=0; r<routerCount; r++) {
		    	//accumulate all the from pipes for a given router group
		    	fromModule[r] = PronghornStage.join(fromModule[r], outputPipes[r]);
		    }
		    
		}
		
		
		return routerConfig;
	}

	private static boolean validateNoNulls(Pipe<ServerResponseSchema>[] outputPipes) {
		
		int i = outputPipes.length;
		while (--i>=0) {
			if (outputPipes[i]==null) {
				throw new NullPointerException("null discovered in output pipe at index "+i);
			}
			
		}
		return true;
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
			 coordinator.processNota(graphManager, unwrapStage);
		}
		
		return handshakeIncomingGroup;
	}

	public static String bindHost() {
		String bindHost;
		boolean noIPV6 = true;//TODO: we really do need to add ipv6 support.
		List<InetAddress> addrList = NetGraphBuilder.homeAddresses(noIPV6);
		if (addrList.isEmpty()) {
			bindHost = "127.0.0.1";
		} else {
			bindHost = addrList.get(0).toString().replace("/", "");
		}
		return bindHost;
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
	

	public static ServerCoordinator httpServerSetup(boolean isTLS, String bindHost, int port, GraphManager gm, 
			                                        boolean large, ModuleConfig modules) {
		
		ServerPipesConfig serverConfig = new ServerPipesConfig(large, isTLS);
				 
		 //This must be large enough for both partials and new handshakes.
	
		ServerCoordinator serverCoord = new ServerCoordinator(isTLS, bindHost, port, serverConfig.maxConnectionBitsOnServer, serverConfig.maxPartialResponsesServer, serverConfig.processorCount);
		
		buildHTTPServerGraph(gm, modules, serverCoord, serverConfig);
		
		return serverCoord;
	}
	
	public static void telemetryServerSetup(boolean isTLS, String bindHost, int port, GraphManager gm) {
	
		logger.info("begin telemetry definition");
		
		final long rate = 40_000_000; //fastest rate in NS
		
		boolean isLarge = false;
		

		
		final ModuleConfig modules = buildTelemetryModuleConfig(rate);
		final ServerPipesConfig serverConfig = new ServerPipesConfig(isLarge, isTLS, 2);
				 
		serverConfig.ensureServerCanWrite(1<<19);//512K
		 //This must be large enough for both partials and new handshakes.
		
		ServerCoordinator serverCoord = new ServerCoordinator(isTLS, bindHost, port, 
				                                              serverConfig.maxConnectionBitsOnServer, 
				                                              serverConfig.maxPartialResponsesServer, 
				                                              serverConfig.processorCount,
				                                              "Telemetry Server","");
		
		serverCoord.setStageNotaProcessor(new PronghornStageProcessor() {
			//force all these to be hidden as part of the monitoring system
			@Override
			public void process(GraphManager gm, PronghornStage stage) {
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, stage);
				//TODO: also use this to set the RATE and elminate the extra argument passed down....
				GraphManager.addNota(gm, GraphManager.MONITOR, GraphManager.MONITOR, stage);
			}
		});

		final ServerFactory factory = new ServerFactory() {

			@Override
			public void buildServer(GraphManager gm, ServerCoordinator coordinator,
					Pipe<ReleaseSchema>[] releaseAfterParse, Pipe<NetPayloadSchema>[] receivedFromNet,
					Pipe<NetPayloadSchema>[] sendingToNet) {
				NetGraphBuilder.buildHTTPStages(gm, coordinator, modules, serverConfig, releaseAfterParse, receivedFromNet, sendingToNet, rate);
			}			
		};
		
		NetGraphBuilder.buildServerGraph(gm, serverCoord, serverConfig, rate, factory);
		
		logger.info("finish telemetry definition"); 
	}

	private static ModuleConfig buildTelemetryModuleConfig(final long rate) {
		ModuleConfig config = new ModuleConfig(){

			private final int moduleCount = 5;
			
			public CharSequence getPathRoute(int a) {
				switch(a) {
					case 0:
						return "/";
					case 1:
						return "/viz-lite.js";
					case 2:
						return "/graph.dot";
					case 3:
						return "/jquery-3.2.1.min.js";
					case 4:
						return "/webworker.js";
					default:
						throw new RuntimeException("unknown value "+a);
				}
			}
		
			@Override
			public int moduleCount() {
				return moduleCount;
			}

			@Override
			public Pipe<ServerResponseSchema>[] registerModule(int a,
					GraphManager graphManager, RouterStageConfig routerConfig, Pipe<HTTPRequestSchema>[] inputPipes) {
				
				//the file server is stateless therefore we can build 1 instance for every input pipe
				int instances = inputPipes.length;
				final int outputPipeChunkMax = 1<<19; //512K
				final int outputPipeChunkMin = 1<<14; //16K
				
				Pipe<ServerResponseSchema>[] staticFileOutputs = new Pipe[instances];
				
				int i = instances;
				while (--i>=0) {
					PronghornStage activeStage = null;
					switch (a) {
						case 0:
						activeStage = ResourceModuleStage.newInstance(graphManager, 
								inputPipes[i], 
								staticFileOutputs[i] = ServerResponseSchema.instance.newPipe(2, outputPipeChunkMin), 
								(HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults>) ((HTTP1xRouterStageConfig)routerConfig).httpSpec,
								"telemetry/index.html", HTTPContentTypeDefaults.HTML);						
						break;
						case 1:
						activeStage = ResourceModuleStage.newInstance(graphManager, 
						          inputPipes[i], 
						          staticFileOutputs[i] = ServerResponseSchema.instance.newPipe(2, outputPipeChunkMax), 
						          ((HTTP1xRouterStageConfig)routerConfig).httpSpec,
						          "telemetry/viz-lite.js", HTTPContentTypeDefaults.JS);
						break;
						case 2:
						activeStage = DotModuleStage.newInstance(graphManager, 
								inputPipes[i], 
								staticFileOutputs[i] = ServerResponseSchema.instance.newPipe(2, outputPipeChunkMax), 
								((HTTP1xRouterStageConfig)routerConfig).httpSpec);
						break;
						case 3:
							activeStage = ResourceModuleStage.newInstance(graphManager, 
							          inputPipes[i], 
							          staticFileOutputs[i] = ServerResponseSchema.instance.newPipe(2, outputPipeChunkMax), 
							          ((HTTP1xRouterStageConfig)routerConfig).httpSpec,
							          "telemetry/jquery-3.2.1.min.js", HTTPContentTypeDefaults.JS);
						break;
						case 4:
							activeStage = ResourceModuleStage.newInstance(graphManager, 
							          inputPipes[i], 
							          staticFileOutputs[i] = ServerResponseSchema.instance.newPipe(2, outputPipeChunkMin), 
							          ((HTTP1xRouterStageConfig)routerConfig).httpSpec,
							          "telemetry/webworker.js", HTTPContentTypeDefaults.JS);
						break;
						//TODO: add version...
						default:
							throw new RuntimeException("unknonw idx "+a);
					}
					
					if (null!=activeStage) {
						GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, activeStage);
				    	GraphManager.addNota(graphManager, GraphManager.MONITOR, GraphManager.MONITOR, activeStage);						
					}
					
				}
				
				routerConfig.registerRoute(
									             getPathRoute(a)
									             ); //no headers

				return staticFileOutputs;			
			}
		};
		return config;
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
		buildHTTPClientGraph(gm, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
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
	
	public static void buildHTTPClientGraph(GraphManager gm, 
			final Pipe<NetResponseSchema>[] httpResponsePipe, Pipe<ClientHTTPRequestSchema>[] requestsPipe,
			int maxPartialResponses, int connectionsInBits, int clientRequestCount, int clientRequestSize,
			boolean isTLS) {
		
		ClientCoordinator ccm = new ClientCoordinator(connectionsInBits, maxPartialResponses, isTLS);
				
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {

			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, 
								    Pipe<NetPayloadSchema>[] clearResponse,
								    Pipe<ReleaseSchema> ackReleaseForResponseParser) {
				
				NetGraphBuilder.buildHTTP1xResponseParser(gm, ccm, httpResponsePipe, clearResponse, ackReleaseForResponseParser);
			}			
		};

		Pipe<NetPayloadSchema>[] clientRequests = Pipe.buildPipes(requestsPipe.length, NetPayloadSchema.instance.<NetPayloadSchema>newPipeConfig(clientRequestCount,clientRequestSize));
				
		buildSimpleClientGraph(gm, ccm, factory, clientRequests);
		
		new HTTPClientRequestStage(gm, ccm, requestsPipe, clientRequests);
	}

	public static Pipe<TwitterEventSchema> buildTwitterUserStream(GraphManager gm, String consumerKey, String consumerSecret, String token, String secret) {
		
		////////////////////////////
		//pipes for holding all HTTPs client requests
		///////////////////////////*            
		int maxRequesters = 1;
		int clientRequestsCount = 8;
		int clientRequestSize = 1<<12;		
		Pipe<ClientHTTPRequestSchema>[] clientRequestsPipes = Pipe.buildPipes(maxRequesters, new PipeConfig<ClientHTTPRequestSchema>(ClientHTTPRequestSchema.instance, clientRequestsCount, clientRequestSize));
		
		////////////////////////////
		//pipes for holding all HTTPs responses from server
		///////////////////////////      
		int maxListeners =  1;
		int clientResponseCount = 32;
		int clientResponseSize = 1<<17;
		Pipe<NetResponseSchema>[] clientResponsesPipes = Pipe.buildPipes(maxListeners, new PipeConfig<NetResponseSchema>(NetResponseSchema.instance, clientResponseCount, clientResponseSize));
		
		////////////////////////////
		//standard HTTPs client subgraph building with TLS handshake logic
		///////////////////////////   
		int maxPartialResponses = 1;
		NetGraphBuilder.buildHTTPClientGraph(gm, maxPartialResponses, clientResponsesPipes, clientRequestsPipes); 
		
		////////////////////////
		//twitter specific logic
		////////////////////////
		int tweetsCount = 32;
		
		Pipe<TwitterStreamControlSchema> streamControlPipe = TwitterStreamControlSchema.instance.newPipe(8, 0);
		final int HTTP_REQUEST_RESPONSE_USER_ID = 0;
		
		////////////////////
		//Stage will open the Twitter stream and reconnect it upon request
		////////////////////		
		new RequestTwitterUserStreamStage(gm, consumerKey, consumerSecret, token, secret, HTTP_REQUEST_RESPONSE_USER_ID, streamControlPipe, clientRequestsPipes[0]);
					
		/////////////////////
		//Stage will parse JSON streaming from Twitter servers and convert it to a pipe containing twitter events
		/////////////////////
		int bottom = 0;//bottom is 0 because response keeps all results at the root
		return TwitterJSONToTwitterEventsStage.buildStage(gm, false, bottom, clientResponsesPipes[HTTP_REQUEST_RESPONSE_USER_ID], streamControlPipe, tweetsCount);
	
	}
	
	public static Pipe<TwitterEventSchema>[] buildTwitterQueryStream(GraphManager gm, 
																    String[] queryText, int[] queryRoutes,
			                                                        String consumerKey, String consumerSecret) {


		int maxQRoute = 0;
		int j = queryRoutes.length;
		while (--j>=0) {
			maxQRoute = Math.max(maxQRoute, queryRoutes[j]);
		}
				
		final int bearerPipeIdx = maxQRoute+1;
		int maxListeners =  bearerPipeIdx+1;
			
		
		////////////////////////////
		//pipes for holding all HTTPs client requests
		///////////////////////////*            
		int maxRequesters = 1;
		int requesterIdx = 0;
		int clientRequestsCount = 8;
		int clientRequestSize = 1<<12;		
		Pipe<ClientHTTPRequestSchema>[] clientRequestsPipes = Pipe.buildPipes(maxRequesters, new PipeConfig<ClientHTTPRequestSchema>(ClientHTTPRequestSchema.instance, clientRequestsCount, clientRequestSize));
		
		////////////////////////////
		//pipes for holding all HTTPs responses from server
		///////////////////////////      

		int clientResponseCount = 32;
		int clientResponseSize = 1<<17;
		Pipe<NetResponseSchema>[] clientResponsesPipes = Pipe.buildPipes(maxListeners, new PipeConfig<NetResponseSchema>(NetResponseSchema.instance, clientResponseCount, clientResponseSize));
			
		////////////////////////////
		//standard HTTPs client subgraph building with TLS handshake logic
		///////////////////////////   
		int maxPartialResponses = 1;
		NetGraphBuilder.buildHTTPClientGraph(gm, 
				             maxPartialResponses, 
				             clientResponsesPipes, clientRequestsPipes); 
		
		////////////////////////
		//twitter specific logic
		////////////////////////
		
		final int tweetsCount = 32;
		final int bottom = 2;//bottom is 2 because multiple responses are wrapped in an array
		int queryGroups = maxQRoute+1;
		
		Pipe<TwitterStreamControlSchema>[] controlPipes = new Pipe[queryGroups];
		Pipe<TwitterEventSchema>[] eventPipes = new Pipe[queryGroups];
		int k = queryGroups;
		while (--k>=0) {
			//we use a different JSON parser for each group of queries.			
			eventPipes[k] = TwitterJSONToTwitterEventsStage.buildStage(gm, true, bottom, 
											clientResponsesPipes[k], 
											controlPipes[k] = TwitterStreamControlSchema.instance.newPipe(8, 0), 
											tweetsCount);
		}
			
		RequestTwitterQueryStreamStage.newInstance(gm, consumerKey, consumerSecret,
											tweetsCount, queryRoutes, queryText,
				                            bearerPipeIdx,
				                            //the parser detected bad bearer or end of data, reconnect
				                            //also sends the PostIds of every post decoded
				                            controlPipes, 
				                            clientResponsesPipes[bearerPipeIdx], //new bearer response
				                            clientRequestsPipes[requesterIdx]); //requests bearers and queries

		return eventPipes;		
		
	}
	
}
