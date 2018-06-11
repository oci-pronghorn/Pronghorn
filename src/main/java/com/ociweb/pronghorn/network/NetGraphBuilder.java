package com.ociweb.pronghorn.network;

import java.io.File;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.http.HTTP1xResponseParserStage;
import com.ociweb.pronghorn.network.http.HTTP1xRouterStage;
import com.ociweb.pronghorn.network.http.HTTP1xRouterStageConfig;
import com.ociweb.pronghorn.network.http.HTTPClientRequestStage;
import com.ociweb.pronghorn.network.http.HTTPLogUnificationStage;
import com.ociweb.pronghorn.network.http.HTTPRequestJSONExtractionStage;
import com.ociweb.pronghorn.network.http.ModuleConfig;
import com.ociweb.pronghorn.network.http.RouterStageConfig;
import com.ociweb.pronghorn.network.module.DotModuleStage;
import com.ociweb.pronghorn.network.module.FileReadModuleStage;
import com.ociweb.pronghorn.network.module.ResourceModuleStage;
import com.ociweb.pronghorn.network.module.SummaryModuleStage;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPLogRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPLogResponseSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.network.schema.TwitterEventSchema;
import com.ociweb.pronghorn.network.schema.TwitterStreamControlSchema;
import com.ociweb.pronghorn.network.twitter.RequestTwitterQueryStreamStage;
import com.ociweb.pronghorn.network.twitter.RequestTwitterUserStreamStage;
import com.ociweb.pronghorn.network.twitter.TwitterJSONToTwitterEventsStage;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeConfigManager;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.file.FileBlobWriteStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorCollectorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.IPv4Tools;
import com.ociweb.pronghorn.util.TrieParserReader;

public class NetGraphBuilder {
	
	private static final Logger logger = LoggerFactory.getLogger(NetGraphBuilder.class);	
	
	/**
	 * This method is only for GreenLighting deep integration and should not be used
	 * unless you want to take responsibility for the handshake activity
	 */
	public static void buildHTTPClientGraph(GraphManager gm, 
			ClientCoordinator ccm, int responseQueue,
			final Pipe<NetPayloadSchema>[] requests, 
			final Pipe<NetResponseSchema>[] responses,
			int netResponseCount,
			int releaseCount, 
			int responseUnwrapCount, 
			int clientWrapperCount,
			int clientWriters) {
		
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {

			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, 
								    Pipe<NetPayloadSchema>[] clearResponse,
								    Pipe<ReleaseSchema> ackReleaseForResponseParser) {
				
				buildHTTP1xResponseParser(gm, ccm, responses, clearResponse, ackReleaseForResponseParser);
			}
			
		};
		buildClientGraph(gm, ccm, responseQueue, requests, 
				         responseUnwrapCount, clientWrapperCount, clientWriters, 
				             releaseCount, netResponseCount, factory);
	}
	
	public static void buildClientGraph(GraphManager gm, ClientCoordinator ccm, 
										int responseQueue,
										Pipe<NetPayloadSchema>[] requests,
										final int responseUnwrapCount,
										int clientWrapperCount,
										int clientWriters,
										int releaseCount, 
										int netResponseCount, 
										ClientResponseParserFactory parserFactory
										) {
	
		int maxPartialResponses = ccm.resposePoolSize();
		
		PipeConfig<ReleaseSchema> parseReleaseConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance, releaseCount, 0);
		
		//must be large enough for handshake plus this is the primary pipe after the socket so it must be a little larger.
		PipeConfig<NetPayloadSchema> clientNetResponseConfig = new PipeConfig<NetPayloadSchema>(
				NetPayloadSchema.instance, responseQueue, ccm.receiveBufferSize); 	
		
		
		//pipe holds data as it is parsed so making it larger is helpful
		PipeConfig<NetPayloadSchema> clientHTTPResponseConfig = new PipeConfig<NetPayloadSchema>(
				NetPayloadSchema.instance, netResponseCount, ccm.receiveBufferSize); 	
		
		
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
		
		//This makes a big difference in testing... TODO: clean this up and develop a new approach.		
		//GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, ((Number)GraphManager.getNota(gm,socketReaderStage.stageId,GraphManager.SCHEDULE_RATE,null)).longValue()/10, socketReaderStage);
		
		
		Pipe<NetPayloadSchema>[] hanshakePipes = buildClientUnwrap(gm, ccm, requests, responseUnwrapCount, socketResponse, clearResponse, acks);	

		buildClientWrapAndWrite(gm, ccm, requests, clientWrapperCount, clientWriters, hanshakePipes);	    

		parserFactory.buildParser(gm, ccm, clearResponse, ackReleaseForResponseParser);
	    
	}

	private static Pipe<NetPayloadSchema>[] buildClientUnwrap(GraphManager gm, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] requests,
			int responseUnwrapCount, Pipe<NetPayloadSchema>[] socketResponse, Pipe<NetPayloadSchema>[] clearResponse,
			Pipe<ReleaseSchema>[] acks) {
		Pipe<NetPayloadSchema>[] hanshakePipes = null;
		if (ccm.isTLS) {
			assert(socketResponse.length>=responseUnwrapCount) : "Can not split "+socketResponse.length+" repsonse pipes across "+responseUnwrapCount+" decrypt units";			
			
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
			int clientWrapperCount, int clientWriters, Pipe<NetPayloadSchema>[] hanshakePipes) {
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
				ClientSocketWriterStage socketWriteStage = new ClientSocketWriterStage(gm, ccm, clientRequests[i]);
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

	public static GraphManager buildHTTPServerGraph(final GraphManager graphManager, 
			                                        final ModuleConfig modules, 
			                                        final ServerCoordinator coordinator) {

		final ServerFactory factory = new ServerFactory() {

			@Override
			public void buildServer(GraphManager gm, ServerCoordinator coordinator,
					Pipe<ReleaseSchema>[] releaseAfterParse, Pipe<NetPayloadSchema>[] receivedFromNet,
					Pipe<NetPayloadSchema>[] sendingToNet) {
				
				buildHTTPStages(gm, coordinator, modules, 
						        releaseAfterParse, receivedFromNet, sendingToNet);
			}
			
		};
				
        return buildServerGraph(graphManager, coordinator, factory);
        
	}

	public static GraphManager buildServerGraph(final GraphManager graphManager,
													final ServerCoordinator coordinator,
													ServerFactory factory) {
		logger.info("building server graph");
		final Pipe<NetPayloadSchema>[] encryptedIncomingGroup = Pipe.buildPipes(coordinator.maxConcurrentInputs, coordinator.incomingDataConfig);           
           
        Pipe<ReleaseSchema>[] releaseAfterParse = buildSocketReaderStage(graphManager, coordinator, coordinator.moduleParallelism(),
        		                                                         encryptedIncomingGroup);
                       
        Pipe<NetPayloadSchema>[] handshakeIncomingGroup=null;
        Pipe<NetPayloadSchema>[] receivedFromNet;
        
        if (coordinator.isTLS) {
        	receivedFromNet = Pipe.buildPipes(	coordinator.maxConcurrentInputs,
        										coordinator.incomingDataConfig);
        	handshakeIncomingGroup = populateGraphWithUnWrapStages(graphManager, coordinator, 
							        			coordinator.serverRequestUnwrapUnits, 
							        			coordinator.incomingDataConfig,
        			                      encryptedIncomingGroup, receivedFromNet, releaseAfterParse);
        } else {
        	receivedFromNet = encryptedIncomingGroup;
        }
		
	
        Pipe<NetPayloadSchema>[] fromOrderedContent = buildRemainderOFServerStages(
        		                              graphManager, coordinator, 
        		                              handshakeIncomingGroup);
        

        factory.buildServer(graphManager, coordinator, 
        		            releaseAfterParse,
        		            receivedFromNet, 
        		            fromOrderedContent);

        return graphManager;
	}

	public static void buildHTTPStages(GraphManager graphManager, ServerCoordinator coordinator, ModuleConfig modules,
			Pipe<ReleaseSchema>[] releaseAfterParse, Pipe<NetPayloadSchema>[] receivedFromNet,
			Pipe<NetPayloadSchema>[] sendingToNet) {
		assert(sendingToNet.length >= coordinator.moduleParallelism()) : "reduce track count since we only have "+sendingToNet.length+" pipes";

		
		HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> httpSpec = HTTPSpecification.defaultSpec();
		
		if (modules.moduleCount()==0) {
			throw new UnsupportedOperationException("Must be using at least 1 module to startup.");
		}
		
		//logger.info("build modules");
        Pipe<ServerResponseSchema>[][] fromModule = new Pipe[coordinator.moduleParallelism()][];       
        Pipe<HTTPRequestSchema>[][] toModules = new Pipe[coordinator.moduleParallelism()][];
                
        final HTTP1xRouterStageConfig routerConfig = buildModules(coordinator, graphManager,
        		 									modules, httpSpec, fromModule, toModules);
		
        //logger.info("build http stages 3");
        
		Pipe<HTTPLogRequestSchema>[] reqLog = new Pipe[coordinator.moduleParallelism()];
		
		Pipe<HTTPLogResponseSchema>[] resLog = new Pipe[coordinator.moduleParallelism()];
		
		Pipe<NetPayloadSchema>[][] perTrackFromNet 
			= Pipe.splitPipes(coordinator.moduleParallelism(), receivedFromNet);
		
		Pipe<NetPayloadSchema>[][] perTrackFromSuper 
			= Pipe.splitPipes(coordinator.moduleParallelism(), sendingToNet);
		
		buildLogging(graphManager, coordinator, reqLog, resLog);
		
		buildRouters(graphManager, coordinator, releaseAfterParse,
				fromModule, toModules, routerConfig, 
				false, reqLog, perTrackFromNet);
		
		//logger.info("build http ordering supervisors");
		buildOrderingSupers(graphManager, coordinator, fromModule, resLog, perTrackFromSuper);
	}

	public static PipeConfig<HTTPRequestSchema> buildRoutertoModulePipeConfig(
			ServerCoordinator coordinator,
			ServerPipesConfig serverConfig) {
		
		final int maxSize = serverConfig.fromRouterToModuleBlob 
        		          + coordinator.connectionStruct().inFlightPayloadSize();
        
        PipeConfig<HTTPRequestSchema> routerToModuleConfig = new PipeConfig<HTTPRequestSchema>(
        		HTTPRequestSchema.instance, 
        		serverConfig.fromRouterToModuleCount, maxSize);///if payload is smaller than average file size will be slower
		
        return routerToModuleConfig;
	}

	public static void buildLogging(GraphManager graphManager,
			                        ServerCoordinator coordinator, 
			                        Pipe<HTTPLogRequestSchema>[] logReq,
			                        Pipe<HTTPLogResponseSchema>[] logRes) {
		
		if (coordinator.isLogFilesEnabled()) {

			LogFileConfig logFileConfig = coordinator.getLogFilesConfig();
			
			//walk the matching pipes and create monitor pipes
			int i = logReq.length;
			while (--i>=0) {
				logReq[i] = HTTPLogRequestSchema.instance.newPipe(32, 1<<12);	
			}
			
			if (logFileConfig.logResponses()) {
				int resTotal = 0;
				i = logRes.length;
				while (--i>=0) {	
					logRes[i] = HTTPLogResponseSchema.instance.newPipe(32, 1<<12);
				}		
			} else {
				//collector should not look for any of these
				logRes = new Pipe[0];
			}
		
			//write large blocks out to the file, logger unification will fill each message
			//NOTE: the file writer should do this however we do not know if the real destination
			//do not set smaller than 1<<19 1/4 meg or volume is impacted
			Pipe<RawDataSchema> out = RawDataSchema.instance.newPipe(8, 1<<20);
			
			//ConsoleJSONDumpStage.newInstance(graphManager, reqIn);
			//ConsoleJSONDumpStage.newInstance(graphManager, resIn);
			
			//PipeCleanerStage.newInstance(graphManager, reqIn);
			//PipeCleanerStage.newInstance(graphManager, resIn);
			
			new HTTPLogUnificationStage(graphManager, logReq, logRes, out);
						
			boolean append = false;
			new FileBlobWriteStage(graphManager, out, 
								   logFileConfig.maxFileSize()
					               ,append, 
					               logFileConfig.base(),
					               logFileConfig.countOfFiles()
					);
			
			//PipeCleanerStage.newInstance(graphManager, out);
			//ConsoleJSONDumpStage.newInstance(graphManager, out);			
			//ConsoleSummaryStage.newInstance(graphManager, out);
			
		}
	}

	public static void buildRouters(GraphManager graphManager, ServerCoordinator coordinator,
			Pipe<ReleaseSchema>[] releaseAfterParse, Pipe<ServerResponseSchema>[][] fromModule,
			Pipe<HTTPRequestSchema>[][] toModules, final HTTP1xRouterStageConfig routerConfig,
			boolean captureAll, Pipe<HTTPLogRequestSchema>[] log,
			Pipe[][] perTrackFromNet) {
		/////////////////////
		//create the routers
		/////////////////////
		
		PipeConfig<ServerResponseSchema> config = coordinator.pcm.getConfig(ServerResponseSchema.class);

		
		int acksBase = releaseAfterParse.length-1;
		int parallelTrack = toModules.length; 
		while (--parallelTrack>=0) { 
									
			//////////////////////
			//as needed we inject the JSON Extractor
			//////////////////////
			
			Pipe<HTTPRequestSchema> prev = null;
			Pipe<HTTPRequestSchema>[] fromRouter = toModules[parallelTrack];
			int routeId = fromRouter.length;
			while (--routeId>=0) {
				
				Pipe<HTTPRequestSchema> pipe = fromRouter[routeId];
				if (pipe==prev){
					//already done since we have multiple paths per route.
					fromRouter[routeId] = fromRouter[routeId+1];
					continue;
				}
				prev = fromRouter[routeId];
				
				JSONExtractorCompleted extractor = routerConfig.JSONExtractor(routeId);
				if (null != extractor) {
					
					////grow from module pipes to have one more error pipe
					Pipe<ServerResponseSchema> json404Pipe = new Pipe<ServerResponseSchema>(config);
					fromModule[parallelTrack] = PronghornStage.join(json404Pipe, fromModule[parallelTrack]);
			        ////////////
					
					Pipe<HTTPRequestSchema> newFromJSON = 
							new Pipe<HTTPRequestSchema>( pipe.config() );
		
					new HTTPRequestJSONExtractionStage(
							 	graphManager, 
							 	extractor, 
							 	routerConfig.getStructIdForRouteId(routeId),
							 	newFromJSON,
							 	fromRouter[routeId],
							 	json404Pipe
							);
					
					fromRouter[routeId] = newFromJSON;
									
				}
			}	
			////////////////////////////////////
			////////////////////////////////////
			
		    ////grow from module pipes to have one more error pipe
			Pipe<ServerResponseSchema> router404Pipe = new Pipe<ServerResponseSchema>(config);
			fromModule[parallelTrack] = PronghornStage.join(router404Pipe, fromModule[parallelTrack]);
			//////////////////
			
			HTTP1xRouterStage router = HTTP1xRouterStage.newInstance(
					graphManager, 
					parallelTrack, 
					perTrackFromNet[parallelTrack], 
					fromRouter, 
					router404Pipe, 
					log[parallelTrack],
					releaseAfterParse[acksBase-parallelTrack],
					routerConfig,
					coordinator,captureAll);        
			
			GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "HTTPParser", router);
			coordinator.processNota(graphManager, router);
		}
	}

	public static Pipe<ReleaseSchema>[] buildSocketReaderStage(GraphManager graphManager, ServerCoordinator coordinator, final int routerCount,
			Pipe<NetPayloadSchema>[] encryptedIncomingGroup) {
		int a = routerCount+(coordinator.isTLS?coordinator.serverRequestUnwrapUnits:0);
		Pipe<ReleaseSchema>[] acks = new Pipe[a];
		while (--a>=0) {
			acks[a] =  new Pipe<ReleaseSchema>(coordinator.pcm.getConfig(ReleaseSchema.class), false);	
		}
                   
        //reads from the socket connection
        ServerSocketReaderStage readerStage = new ServerSocketReaderStage(graphManager, acks, encryptedIncomingGroup, coordinator);
        GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketReader", readerStage);
        coordinator.processNota(graphManager, readerStage);
		return acks;
	}

	public static Pipe<NetPayloadSchema>[] buildRemainderOFServerStages(final GraphManager graphManager,
			ServerCoordinator coordinator, Pipe<NetPayloadSchema>[] handshakeIncomingGroup) {

		Pipe<NetPayloadSchema>[] fromOrderedContent = new Pipe[
		                       coordinator.serverResponseWrapUnitsAndOutputs 
		                       * coordinator.serverPipesPerOutputEngine];

		Pipe<NetPayloadSchema>[] toWiterPipes = buildSSLWrapersAsNeeded(graphManager, coordinator, 
				                                                       handshakeIncomingGroup,
				                                                       fromOrderedContent);
                    
        buildSocketWriters(graphManager, coordinator, coordinator.serverSocketWriters, toWiterPipes);

        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator); 
        coordinator.processNota(graphManager, newConStage);

		return fromOrderedContent;
	}

	private static Pipe<NetPayloadSchema>[] buildSSLWrapersAsNeeded(final GraphManager graphManager,
			ServerCoordinator coordinator,
			Pipe<NetPayloadSchema>[] handshakeIncomingGroup,
			Pipe<NetPayloadSchema>[] fromOrderedContent) {
		
		
		int requestUnwrapUnits = coordinator.serverRequestUnwrapUnits;		
		int y = coordinator.serverPipesPerOutputEngine;
		int z = coordinator.serverResponseWrapUnitsAndOutputs;
		Pipe<NetPayloadSchema>[] toWiterPipes = null;
		PipeConfig<NetPayloadSchema> fromOrderedConfig = coordinator.pcm.getConfig(NetPayloadSchema.class);
		
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

	public static void buildOrderingSupers(GraphManager graphManager, ServerCoordinator coordinator,
			Pipe<ServerResponseSchema>[][] fromModule, Pipe<HTTPLogResponseSchema>[] log,
			Pipe<NetPayloadSchema>[][] perTrackFromSuper) {
		
		int track = fromModule.length;
		while (--track>=0) {
		
			OrderSupervisorStage wrapSuper = new OrderSupervisorStage(graphManager, 
									                    fromModule[track], 
									                    log[track],
									                    perTrackFromSuper[track], 
									                    coordinator);//ensure order   
	
			coordinator.processNota(graphManager, wrapSuper);
		
		}
	}

	private static void buildSocketWriters(GraphManager graphManager, ServerCoordinator coordinator, 
											int socketWriters, Pipe<NetPayloadSchema>[] toWiterPipes) {
		///////////////
		//all the writer stages
		///////////////
		
		
		Pipe[][] req = Pipe.splitPipes(socketWriters, toWiterPipes);	
		int w = socketWriters;
		while (--w>=0) {
			
			ServerSocketWriterStage writerStage = new ServerSocketWriterStage(graphManager, coordinator, req[w]); //pump bytes out
		    GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketWriter", writerStage);
		   	coordinator.processNota(graphManager, writerStage);
		}
	}

	public static HTTP1xRouterStageConfig buildModules(ServerCoordinator coordinator, GraphManager graphManager, ModuleConfig modules,
			HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> httpSpec,
			Pipe<ServerResponseSchema>[][] fromModule,
			Pipe<HTTPRequestSchema>[][] toModules) {
		
		PipeConfig<HTTPRequestSchema> routerToModuleConfig = coordinator.pcm.getConfig(HTTPRequestSchema.class);
		
		final int routerCount = coordinator.moduleParallelism();

		final HTTP1xRouterStageConfig routerConfig = new HTTP1xRouterStageConfig(httpSpec, coordinator.connectionStruct()); 

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
			Pipe<ServerResponseSchema>[] outputPipes = modules.registerModule(
					                moduleInstance, graphManager, routerConfig, routesTemp);
			
			int maxOut = coordinator.pcm.getConfig(NetPayloadSchema.class).maxVarLenSize();
			int i = outputPipes.length;
			while (--i>=0) {
				if (outputPipes[i].maxVarLen>maxOut) {
					throw new UnsupportedOperationException(
						"Module instance "+moduleInstance+" is configured to write blocks of "+outputPipes[i].maxVarLen+
						" but the server is set to maximum response size of "+maxOut+
						". Either setMaxResponseSize larger or modify module to write less."
					);
				}
			}
			
			
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
			Pipe[] planIncomingGroup, Pipe[] acks) {
		Pipe<NetPayloadSchema>[] handshakeIncomingGroup = new Pipe[requestUnwrapUnits];
		            	
		int c = requestUnwrapUnits;
		Pipe[][] in = Pipe.splitPipes(c, encryptedIncomingGroup);
		Pipe[][] out = Pipe.splitPipes(c, planIncomingGroup);
		
		while (--c>=0) {
			handshakeIncomingGroup[c] = new Pipe(handshakeDataConfig);
			SSLEngineUnWrapStage unwrapStage = new SSLEngineUnWrapStage(graphManager, coordinator, in[c], out[c], acks[c], handshakeIncomingGroup[c], true, 0);
			GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "UnWrap", unwrapStage);
			coordinator.processNota(graphManager, unwrapStage);
		}
		
		return handshakeIncomingGroup;
	}

	public static String bindHost(String bindHost) {
		
		
		TrieParserReader reader = new TrieParserReader(true);
		int token =  null==bindHost?-1:(int)reader.query(IPv4Tools.addressParser, bindHost);
		
		if ((null==bindHost || token>=0) && token<4) {
			boolean noIPV6 = true;//TODO: we really do need to add ipv6 support.
			List<InetAddress> addrList = NetGraphBuilder.homeAddresses(noIPV6);
			bindHost = IPv4Tools.patternMatchHost(reader, token, addrList);
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
	

	public static void telemetryServerSetup(TLSCertificates tlsCertificates, String bindHost, int port,
			                                GraphManager gm, int baseRate) {

		///////////////
		//telemetry latency can be as large as 160ms so we run this sever very slow
		//this ensures that the application runs normally without starvation 
		///////////////
		//The graph.dot must respond in less than 40ms but 20ms is nominal
		///////////////
		final int serverRate = baseRate>>6;

		final ModuleConfig modules = buildTelemetryModuleConfig(serverRate);
		boolean isTLS = tlsCertificates != null;
		int maxConnectionBits = 12;
		int tracks = 1;
		
		int concurrentChannelsPerDecryptUnit = 8; //need large number for new requests
		int concurrentChannelsPerEncryptUnit = 1; //this will use a lot of memory if increased
				
		int maxRequestSize = 1<<16; //for cookies sent in
		
		
		HTTPServerConfig c = NetGraphBuilder.serverConfig(8080, gm);
		if (!isTLS) {
			c.useInsecureServer();
		}
		c.setEncryptionUnitsPerTrack(2);
		c.setConcurrentChannelsPerEncryptUnit(concurrentChannelsPerEncryptUnit);
		c.setDecryptionUnitsPerTrack(1);
		c.setConcurrentChannelsPerDecryptUnit(concurrentChannelsPerDecryptUnit);
		c.setMaxConnectionBits(maxConnectionBits);
		c.setMaxRequestSize(maxRequestSize);
		//c.setMaxResponseSize(1<<15);
		c.setTracks(tracks);
		((HTTPServerConfigImpl)c).finalizeDeclareConnections();		
		
		final ServerPipesConfig serverConfig = c.buildServerConfig();
		
		//This must be large enough for both partials and new handshakes.
		serverConfig.ensureServerCanWrite(1<<20);//1MB out
		ServerConnectionStruct scs = new ServerConnectionStruct(gm.recordTypeData);
		ServerCoordinator serverCoord = new ServerCoordinator(tlsCertificates,
				        bindHost, port, scs,								
				        false, "Telemetry Server","", serverConfig);

		
		
		serverCoord.setStageNotaProcessor(new PronghornStageProcessor() {
			//force all these to be hidden as part of the monitoring system
			@Override
			public void process(GraphManager gm, PronghornStage stage) {
				
				int divisor = 1;
				int multiplier = 1;
				
					
				if (stage instanceof ServerNewConnectionStage 
				 || stage instanceof ServerSocketReaderStage) {
					multiplier = 2; //rarely check since this is telemetry
				} else {
					
					int inC = GraphManager.getInputPipeCount(gm, stage.stageId);
					if (inC>1) {
						//if we join N sources all with the same schema.
							
						Pipe<?> basePipe = GraphManager.getInputPipe(gm, stage.stageId, 1);
						
						boolean allPipesAreForSameSchema = true;
						int x = inC+1;
						while(--x > 1) {
							allPipesAreForSameSchema |=
							Pipe.isForSameSchema(basePipe, 
									             (Pipe<?>) GraphManager.getInputPipe(gm, stage.stageId, x));
						}
						if (allPipesAreForSameSchema 
							&& (!(stage instanceof HTTP1xRouterStage))
							&& (!(stage instanceof OrderSupervisorStage))
								) {
							divisor = 1<<(int)(Math.rint(Math.log(inC)/Math.log(2)));
						}
						
					}
					
					
					
				}
				
				
			    //server must be very responsive so it has its own low rate.
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, (multiplier*serverRate)/divisor, 
						stage);

				//TODO: also use this to set the RATE and elminate the extra argument passed down....
				GraphManager.addNota(gm, GraphManager.MONITOR, GraphManager.MONITOR, stage);
			}
		});
		
		final ServerFactory factory = new ServerFactory() {

			@Override
			public void buildServer(GraphManager gm, ServerCoordinator coordinator,
					Pipe<ReleaseSchema>[] releaseAfterParse, Pipe<NetPayloadSchema>[] receivedFromNet,
					Pipe<NetPayloadSchema>[] sendingToNet) {
	
				NetGraphBuilder.buildHTTPStages(gm, coordinator, modules, 
						        releaseAfterParse, receivedFromNet, sendingToNet);
			}			
		};
		
		NetGraphBuilder.buildServerGraph(gm, serverCoord, factory);
	}

	private static ModuleConfig buildTelemetryModuleConfig(final long rate) {
		
		//TODO: this must be large enough or we have issues in FogLight on pi getting dot lite JS
		final int outputPipeChunk = 1 << 20;  //1M //is this too small??  
		
		final int outputPipeGraphChunk = 1<<19;//512K
		
		ModuleConfig config = new ModuleConfig(){
			PipeMonitorCollectorStage monitor;
	
			private final String[] routes = new String[] {
					 "/${path}"			
					,"/graph.dot"
					,"/summary.json"
//					,"/dataView?pipeId=#{pipeId}"
//					,"/histogram/pipeFull?pipeId=#{pipeId}"
//					,"/histogram/stageElapsed?stageId=#{stageId}"
//					,"/WS1/example" //server side websocket example
							
			};
			
			public CharSequence getPathRoute(int a) {
				return routes[a];
			}
	
			@Override
			public int moduleCount() {
				return routes.length;
			}

			@Override
			public Pipe<ServerResponseSchema>[] registerModule(int a,
					GraphManager graphManager,
					RouterStageConfig routerConfig,
					Pipe<HTTPRequestSchema>[] inputPipes) {
				
				//the file server is stateless therefore we can build 1 instance for every input pipe
				int instances = inputPipes.length;

				Pipe<ServerResponseSchema>[] staticFileOutputs = new Pipe[instances];
				
					PronghornStage activeStage = null;
					switch (a) {
						case 0:
						activeStage = ResourceModuleStage.newInstance(graphManager, 
								inputPipes, 
								staticFileOutputs = Pipe.buildPipes(instances, 
										 ServerResponseSchema.instance.newPipeConfig(2, outputPipeChunk)), 
								(HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults>) ((HTTP1xRouterStageConfig)routerConfig).httpSpec,
								"telemetry/","index.html");						
						break;

						case 1:
					    if (null==monitor) {	
							monitor = PipeMonitorCollectorStage.attach(graphManager);	
					    }
						activeStage = DotModuleStage.newInstance(graphManager, monitor,
								inputPipes, 
								staticFileOutputs = Pipe.buildPipes(instances, 
										           ServerResponseSchema.instance.newPipeConfig(2, outputPipeGraphChunk)), 
								((HTTP1xRouterStageConfig)routerConfig).httpSpec);
						break;
						case 2:
						    if (null==monitor) {	
								monitor = PipeMonitorCollectorStage.attach(graphManager);	
						    }
							activeStage = SummaryModuleStage.newInstance(graphManager, monitor,
									inputPipes, 
									staticFileOutputs = Pipe.buildPipes(instances, 
											           ServerResponseSchema.instance.newPipeConfig(2, outputPipeGraphChunk)), 
									((HTTP1xRouterStageConfig)routerConfig).httpSpec);
							break;
//						case 3:
//						
//					
//							//One module for each file??
//							//TODO: add monitor to this stream
//							//this will be a permanent output stream...
//							//Pipe<?> results = PipeMonitor.addMonitor(getPipe(graphManager, pipeId));
//							//some stage much stream out this pipe?
//							//must convert to JSON and stream.
//							//how large is the JSON blocks must ensure output is that large.
//							
//							//TODO: replace this code with the actual streaming data from pipe..
//							activeStage = new DummyRestStage(graphManager, 
//									                          inputPipes, 
//									                          staticFileOutputs = Pipe.buildPipes(instances, 
//																           ServerResponseSchema.instance.newPipeConfig(2, outputPipeChunk)), 
//									                          ((HTTP1xRouterStageConfig)routerConfig).httpSpec);
//							break;
//						case 4:
//						//TODO: replace this code with the actual pipe full histogram
//							activeStage = new DummyRestStage(graphManager, 
//			                          inputPipes, 
//			                          staticFileOutputs = Pipe.buildPipes(instances, 
//									           ServerResponseSchema.instance.newPipeConfig(2, outputPipeChunk)), 
//			                          ((HTTP1xRouterStageConfig)routerConfig).httpSpec);
//							break;
//						case 5:
//						//TODO: replace this code with the actual stage elapsed histogram
//							activeStage = new DummyRestStage(graphManager, 
//			                          inputPipes, 
//			                          staticFileOutputs = Pipe.buildPipes(instances, 
//									           ServerResponseSchema.instance.newPipeConfig(2, outputPipeChunk)), 
//			                          ((HTTP1xRouterStageConfig)routerConfig).httpSpec);
//							break;
//						case 6:
//						//TODO: replace this code with the actual pipe full histogram
//							
//							activeStage = new UpgradeToWebSocketStage(graphManager, 
//			                          inputPipes, 
//			                          staticFileOutputs = Pipe.buildPipes(instances, 
//									           ServerResponseSchema.instance.newPipeConfig(2, outputPipeChunk)), 
//			                          ((HTTP1xRouterStageConfig)routerConfig).httpSpec);
//							
//							break;
							default:
														
							throw new RuntimeException("unknown idx "+a);
					}
					
					if (null!=activeStage) {
						GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, activeStage);
						GraphManager.addNota(graphManager, GraphManager.MONITOR, GraphManager.MONITOR, activeStage);						
					}
					
				if (a==5) {
					
//				             ,HTTPHeaderDefaults.ORIGIN.rootBytes()
//				             ,HTTPHeaderDefaults.SEC_WEBSOCKET_ACCEPT.rootBytes()
//				             ,HTTPHeaderDefaults.SEC_WEBSOCKET_KEY.rootBytes()
//				             ,HTTPHeaderDefaults.SEC_WEBSOCKET_PROTOCOL.rootBytes()
//				             ,HTTPHeaderDefaults.SEC_WEBSOCKET_VERSION.rootBytes()
//				             ,HTTPHeaderDefaults.UPGRADE.rootBytes()
//				             ,HTTPHeaderDefaults.CONNECTION.rootBytes()

					routerConfig.registerCompositeRoute().path( getPathRoute(a));

				} else {
					
					routerConfig.registerCompositeRoute().path( getPathRoute(a));
					//no headers
				}
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
		final TLSCertificates tlsCertificates = TLSCertificates.defaultCerts;

		buildHTTPClientGraph(gm, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
		 clientRequestCount, clientRequestSize, tlsCertificates);
				
	}

	public static void buildSimpleClientGraph(GraphManager gm, ClientCoordinator ccm,
											  ClientResponseParserFactory factory, 
											  Pipe<NetPayloadSchema>[] clientRequests) {
		int clientWriters = 1;				
		int responseUnwrapCount = 1;
		int clientWrapperCount = 1;
		int responseQueue = 10;
		int releaseCount = 2048;
		int netResponseCount = 64;
		int netResponseBlob = 1<<19;
				
		buildClientGraph(gm, ccm, responseQueue, clientRequests, responseUnwrapCount, clientWrapperCount, clientWriters,
				         releaseCount, netResponseCount, factory);
	}
	
	/**
	 * This is the method you want for making HTTP calls from the client side
	 * @param gm
	 * @param httpResponsePipe
	 * @param requestsPipe
	 * @param maxPartialResponses
	 * @param connectionsInBits
	 * @param clientRequestCount
	 * @param clientRequestSize
	 * @param tlsCertificates
	 */
	public static void buildHTTPClientGraph(GraphManager gm, 
			final Pipe<NetResponseSchema>[] httpResponsePipe, Pipe<ClientHTTPRequestSchema>[] requestsPipe,
			int maxPartialResponses, int connectionsInBits, int clientRequestCount, int clientRequestSize,
			TLSCertificates tlsCertificates) {
		
		ClientCoordinator ccm = new ClientCoordinator(connectionsInBits, maxPartialResponses, tlsCertificates, gm.recordTypeData);
				
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

	public static HTTPServerConfig serverConfig(int host, PipeConfigManager pcm, GraphManager gm) {
		return new HTTPServerConfigImpl(host, pcm, gm.recordTypeData);				
	}
	
	public static HTTPServerConfig serverConfig(int host, GraphManager gm) {
		return new HTTPServerConfigImpl(host, new PipeConfigManager(), gm.recordTypeData);				
	}
	
	public static ModuleConfig simpleFileServer(final String pathRoot, final int messagesToOrderingSuper,
			final int messageSizeToOrderingSuper) {
		//using the basic no-fills API
		ModuleConfig config = new ModuleConfig() { 
		
		    //this is the cache for the files, so larger is better plus making it longer helps a lot but not sure why.
		    final PipeConfig<ServerResponseSchema> fileServerOutgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 
		    		         messagesToOrderingSuper, messageSizeToOrderingSuper);//from module to  supervisor
	
			@Override
			public int moduleCount() {
				return 1;
			}
	
			@Override
			public Pipe<ServerResponseSchema>[] registerModule(int a,
					GraphManager graphManager, RouterStageConfig routerConfig, 
					Pipe<HTTPRequestSchema>[] inputPipes) {
				
	
					//the file server is stateless therefore we can build 1 instance for every input pipe
					int instances = inputPipes.length;
					
					Pipe<ServerResponseSchema>[] staticFileOutputs = new Pipe[instances];
					
					int i = instances;
					while (--i>=0) {
						staticFileOutputs[i] = new Pipe<ServerResponseSchema>(fileServerOutgoingDataConfig);
						FileReadModuleStage.newInstance(graphManager, inputPipes[i], staticFileOutputs[i], (HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults>) ((HTTP1xRouterStageConfig)routerConfig).httpSpec, new File(pathRoot));					
					}
						
					routerConfig.registerCompositeRoute().path("/${path}");
					//no headers requested
					
				return staticFileOutputs;
			}        
		 	
		 };
		return config;
	}
	
}
