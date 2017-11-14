package com.ociweb.pronghorn.stage.network;

import com.ociweb.pronghorn.network.*;
import com.ociweb.pronghorn.network.config.*;
import com.ociweb.pronghorn.network.http.HTTP1xRouterStageConfig;
import com.ociweb.pronghorn.network.http.HTTPClientRequestStage;
import com.ociweb.pronghorn.network.http.ModuleConfig;
import com.ociweb.pronghorn.network.http.RouterStageConfig;
import com.ociweb.pronghorn.network.module.FileReadModuleStage;
import com.ociweb.pronghorn.network.schema.*;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

public class HTTPSRoundTripTest {


    static boolean debug = false;
    
	@Ignore
    //@Test
	public void roundTripTest2() {
				

	   
		 //Netty bench 14,000 1m  1.5GB  32users
		  //	ns per call: 5458.1562
		//	calls per sec:   183,212.06
			//mean latency   163,372
			
			//PhogLight  14,000 1m    0.5GB 32 users	
			// mean latency  20,928
			//ns per call: 749.3125
			//calls per sec: 1,334,556.6
			
			//7x faster
			
			////////////////////////////// 8 way client tests.
			
			//nginx    .25GB              
			//ns per call: 3524.0938      3803.0312         
			//calls per sec: 283760.9     262948.12
			
			//3x faster nginx , using 3x memory.
			
			
			//netty  3.3GB                  1.6GB
			// latency  665,299            719,259
			//ns per call: 5463.8125       6023.5312
			//calls per sec: 183022.39   166015.58
			
			//Phog  1.3GB            .79G
			// ltency  113506
			//ns per call: 1080.625    1065.0312
			//calls per sec: 925390.4  938939.56
			
			// 1/2 memory over netty and 5x faster
			
			//feb checks
			//nginx    260K  with  160MB
			
			//GL small   505K        52MB  
			//GL large   1.31M       900M
			//netty      160K        600M  (not TLS) 
			
			//TLS			
			//nginx     100K with     166M ?  4 clients			
			//GL small  120K          600M
			//GL large  124K          1.5GB
			//netty      80K         1.7GB
			
//			#Java comparison of HTTPs TLS requests per second
//			 74K RPS, 549 MB RAM - RxNetty
//			117K RPS, 589 MB RAM - Green Lightning (over 50% more RPS)

		
			
			boolean isTLS = false;
			int port = isTLS?8443:8080;
			String host =  //"10.201.200.24";//phi
					      //"10.10.10.244";
					        "127.0.0.1"; // String host = "10.10.10.134";//" "10.10.10.244";/
			
			boolean isLarge = true;			
			boolean useLocalServer = true;

			final String testFile = "groovySum.json";
			final int loadMultiplier = isTLS? 300_000 : 3_000_000;
			
			roundTripHTTPTest(isTLS, port, host, isLarge, useLocalServer, testFile, loadMultiplier);

	}

	//@Ignore
    @Test
	public void simpleHTTPTest() {
    	boolean isLarge = false;		
    	
    	boolean isTLS = false;
		int port = 8085;
		String host = "127.0.0.1";
		boolean useLocalServer = true;

		final String testFile = "groovySum.json";
		final int loadMultiplier = 2_000;
		
		roundTripHTTPTest(isTLS, port, host, isLarge, useLocalServer, testFile, loadMultiplier);

    }
    
	//@Ignore
    @Test
	public void simpleHTTPSTest() {
    	boolean isLarge = false;		
    	
    	boolean isTLS = true;
		int port = 9443;
		String host = "127.0.0.1";
		boolean useLocalServer = true;

		final String testFile = "groovySum.json";
		final int loadMultiplier = 1_000;
		
		roundTripHTTPTest(isTLS, port, host, isLarge, useLocalServer, testFile, loadMultiplier);

    }
    
    
	private void roundTripHTTPTest(boolean isTLS, int port, String host, 
			                       boolean isLarge, boolean useLocalServer,
			                       final String testFile, final int loadMultiplier) {
	
				GraphManager gm = new GraphManager();
		
				if (debug) {
					//monitor..
					gm.enableTelemetry(8089);
				}
				
				boolean printProgress = false;
				//TODO: will big sleeps show the backed up pipes more clearly? TODO: must be tuned for pipe lenghths?
				
				
				//NOTE: larger values here allows for more effecient scheculeing and bigger "batches"
				//NOTE: smaller values here will allow for slightly lower latency values
				//NOTE: by sleeping less we get more work done per stage, sometimes
				GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 1_200); //this is in ns, can be as low as 1_200
				
				//TODO: we need a better test that has each users interaction of 10 then wait for someone else to get in while still connected.
				//TODO: urgent need to kill off expired pipe usages.
		
				 
				ServerCoordinator serverCoord = null;
				if (useLocalServer) {
					serverCoord = exampleServerSetup(isTLS, gm, testFile, host, port, isLarge);
					
				}
		
				///TODO: when we have more clients than the server has pertials for TLS we can get a repetable hang situation.
				//       confirm hanshake pipe releases
				
				/////////////////
				/////////////////
				int base2SimultaniousConnections = isLarge ? 3 : 1;
				int clientCount = isLarge ? 4 : 2;
					    	
				//TODO: this number must be the limit of max simuantious handshakes.
				int maxPartialResponsesClient = (1<<base2SimultaniousConnections); //input lines to client (should be large)
				final int clientOutputCount = 1<<base2SimultaniousConnections;//should be < client connections,  number of pipes getting wrappers and sent out put stream 
				final int clientWriterStages = 1; //writer instances;	
				
				
				/////////////
				////////////
				
				//client output count of pipes, this is the max count of handshakes from this client since they block all following content.
				
				
				int clientResponseUnwrapUnits = 2;//maxPartialResponsesClient;//To be driven by core count,  this is for consuming get responses
				int clientRequestWrapUnits = 2;//maxPartialResponsesClient;//isTLS?4:8;//To be driven by core count, this is for production of post requests, more pipes if we have no wrappers?
				int requestQueue = 64; 
		
				int responseQueue = 64; //is this used for both socket writer and http builder? seems like this is not even used..
				
				//////////////
		
				final int totalUsersCount = 1<<base2SimultaniousConnections;
		
					    	
				//one of these per unwrap unit and per partial message, there will be many of these normally so they should not be too large
				//however should be deeper if we are sending lots of messages
				int netRespQueue = 8;
				
				//TODO: to ensure we do not loop back arround (overflow bug to be fixed) this value is set large.
				int netRespSize = 1<<18;//must be just larger than the socket buffer 
				//TODO: even with this there is still corruption in the clientSocketReader...
				
				
				ClientCoordinator[] clientCoords = new ClientCoordinator[clientCount];
				RegulatedLoadTestStage[] clients = new RegulatedLoadTestStage[clientCount];
				
				int writeBufferMultiplier = isLarge ? 24 : 8;

				int extraHashBits = 1;
				int requestQueueBytes = 1<<8;
				int responseQueueBytes = 1<<14;
				int httpRequestQueueSize = isTLS? 32 : 128; 
				int httpRequestQueueBytes = isTLS? 1<<15 : 1<<12; //NOTE: Must be 32K for encryption.
				int releaseCount = 2048;
				int netResponseCount = 4;
				int netResponseBlob = 1<<18; //NOTE: must be 256K or larger for decyrption.
								
				
				int cc = clientCount;
				while (--cc>=0) {	    	
					{
						//holds new requests
						Pipe<ClientHTTPRequestSchema>[] input = new Pipe[totalUsersCount];
						
						int usersBits = 0;//2; //TODO: not working they stomp on same client pipe?
						int usersPerPipe = 1<<usersBits;
						TLSCertificates certs = isTLS ? TLSCertificates.defaultCerts : null;
						ClientCoordinator clientCoord1 = new ClientCoordinator(base2SimultaniousConnections+usersBits, maxPartialResponsesClient, certs);
												
						Pipe<NetResponseSchema>[] toReactor = defineClient(isTLS, gm, base2SimultaniousConnections+usersBits+extraHashBits, clientOutputCount, maxPartialResponsesClient, 
								                                           input, clientCoord1, clientResponseUnwrapUnits, clientRequestWrapUnits,
								                                           requestQueue, requestQueueBytes, responseQueue, responseQueueBytes,
								                                           clientWriterStages, netRespQueue, netRespSize, 
								                                           httpRequestQueueSize,httpRequestQueueBytes,writeBufferMultiplier, 
								                                           releaseCount, netResponseCount, netResponseBlob);
						assert(toReactor.length == input.length);
						clients[cc] = new RegulatedLoadTestStage(gm, toReactor, input, totalUsersCount*loadMultiplier, "/"+testFile, usersPerPipe, port, host, "reg"+cc,clientCoord1,printProgress);
						clientCoords[cc]=clientCoord1;
					}
				}
				
				//if (base2SimultaniousConnections<=6) {
					//GraphManager.exportGraphDotFile(gm, "HTTPSRoundTripTest");	
				
				//	MonitorConsoleStage.attach(gm); 
				//	NetGraphBuilder.telemetryServerSetup(false, "127.0.0.1", 8098, gm);
				
					//}
				
				final ServerCoordinator serverCoord1 = serverCoord;
				final ClientCoordinator[] clientCoord = clientCoords;
				final StageScheduler scheduler = new ThreadPerStageScheduler(gm);
						               
				Runtime.getRuntime().addShutdownHook(new Thread() {
				    public void run() {
				    	scheduler.shutdown();
				    	scheduler.awaitTermination(3, TimeUnit.SECONDS);
				    	    if (null!=serverCoord1) {
				    	    	serverCoord1.shutdown();
				    	    }
				            int i = clientCoord.length;
				            while (--i>=0) {
				            	clientCoord[i].shutdown();
				            }
				    }
				});
				
				
				long start = System.currentTimeMillis();
				scheduler.startup();
		
				
				/////////////////
				/////////////////
		
				
				long totalReceived = 0;
				int c = clientCoords.length;
				while (--c>=0) {
					GraphManager.blockUntilStageBeginsShutdown(gm,  clients[c]);	
					clientCoords[c].shutdown();
					totalReceived += clients[c].totalReceived();
				}
		
				long duration = System.currentTimeMillis()-start;
		
				if (null!=serverCoord) {
					serverCoord.shutdown();
				}
		
				
				scheduler.shutdown();
				scheduler.awaitTermination(2, TimeUnit.SECONDS);
				
		
				
		//	hist.outputPercentileDistribution(System.out, 0d);
				
		//	System.out.println("total bytes returned:"+cleaner.getTotalBlobCount()+" expected "+expectedData); //434_070  23_930_000
								
				
				System.out.println("duration: "+duration);
				
				float msPerCall = duration/(float)totalReceived;
				float nsPerCall = 1000000f*msPerCall;
				System.out.println("ns per call: "+nsPerCall);		
				System.out.println("calls per sec: "+(1000f/msPerCall)); //task manager its self can slow down results so avoid running it during test.
	}

	private ServerCoordinator exampleServerSetup(boolean isTLS, GraphManager gm, final String testFile, String bindHost, int bindPort, boolean isLarge) {
		final String pathRoot = buildStaticFileFolderPath(testFile);
				
		System.out.println("init file path "+pathRoot);
		
		//final int maxPartialResponsesServer     = 32; //input lines to server (should be large)
		//final int maxConnectionBitsOnServer 	= 12;//8K simulanious connections on server	    	
		final int messagesToOrderingSuper       = isLarge ? 1<<13 : 1<<8;	    		
		final int messageSizeToOrderingSuper    = isLarge ? 1<<12 : 1<<9;	    		
		

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
						
				
					routerConfig.registerRoute(
                        "/${path}"
						); //no headers requested

				return staticFileOutputs;
			}        
		 	
		 };

		TLSCertificates certs = isTLS ? TLSCertificates.defaultCerts : null;
	
		 ServerCoordinator serverCoord = NetGraphBuilder.httpServerSetup(certs, bindHost, bindPort, gm, isLarge, config);

		 return serverCoord;
	}
	
	
	private String buildStaticFileFolderPath(String testFile) {
		URL dir = ClassLoader.getSystemResource(testFile);
		String root = "";	//file:/home/nate/Pronghorn/target/test-classes/OCILogo.png
			
		try {
		
			String uri = dir.toURI().toString();			
			root = uri.substring("file:".length(), uri.lastIndexOf('/')).replace("%20", " ").replace("/", File.separator).replace("\\", File.separator);
			
		} catch (URISyntaxException e) {						
			e.printStackTrace();
			fail();
		}
		return root;
	}


	private StageScheduler setupScheduler(GraphManager gm, final ServerCoordinator serverCoord, final ClientCoordinator ... clientCoord) {

       //TODO: determine which stages can support batching
		//GraphManager.enableBatching(gm);
		
        final StageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
		//TODO:: fix this to limit threads in use
        //final StageScheduler scheduler = new FixedThreadsScheduler(gm, 16);
                
                       
        //TODO: add this to scheduler so its done everywehre by default!!  TODO: urgent.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                    scheduler.shutdown();
                    scheduler.awaitTermination(3, TimeUnit.SECONDS);

                    serverCoord.shutdown();
                    int i = clientCoord.length;
                    while (--i>=0) {
                    	clientCoord[i].shutdown();
                    }
            }
        });
		return scheduler;
	}
	


	private Pipe<NetResponseSchema>[] defineClient(boolean isTLS, GraphManager gm, int bitsPlusHashRoom,
			int outputsCount, int maxPartialResponses, Pipe<ClientHTTPRequestSchema>[] input, ClientCoordinator ccm, int responseUnwrapUnits, int requestWrapUnits,
			int requestQueue, int requestQueueBytes, int responseQueue, int responseQueueBytes, int clientWriterStages,
			int netRespQueue, int netRespSize, int httpRequestQueueSize, int httpRequestQueueBytes, 
			int writeBufferMultiplier, int releaseCount, int netResponseCount, int netResponseBlob) {
					
		
		//create more pipes if more wrapers were requested.
		if (requestWrapUnits>outputsCount) {
			outputsCount = requestWrapUnits;
		}
		//out to the server, one of these for every client user
		PipeConfig<ClientHTTPRequestSchema> netRequestConfig = new PipeConfig<ClientHTTPRequestSchema>(ClientHTTPRequestSchema.instance, requestQueue, requestQueueBytes);
		//System.err.println("in "+netRequestConfig);
		//back from server, one of these for every client user.
		PipeConfig<NetResponseSchema> netResponseConfig = new PipeConfig<NetResponseSchema>(NetResponseSchema.instance, responseQueue, responseQueueBytes);
		//System.err.println("out "+netResponseConfig);	
		
		//second pipe which also impacts latency		
		PipeConfig<NetPayloadSchema> httpRequestConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance,httpRequestQueueSize,httpRequestQueueBytes); 
		
		//////////////
		//these 2 are small since we have so many
		/////////////
		
		
		//responses from the server	
		final Pipe<NetResponseSchema>[] toReactor = new Pipe[input.length];	
				
		int m = input.length;
		while (--m>=0) {
			toReactor[m] = new Pipe<NetResponseSchema>(netResponseConfig);
			input[m] = new Pipe<ClientHTTPRequestSchema>(netRequestConfig);	
		}

		
		Pipe<NetPayloadSchema>[] clientRequests = new Pipe[outputsCount];
		int r = outputsCount;
		while (--r>=0) {
			clientRequests[r] = new Pipe<NetPayloadSchema>(httpRequestConfig);		
		}
		
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {

			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, 
								    Pipe<NetPayloadSchema>[] clearResponse,
								    Pipe<ReleaseSchema> ackReleaseForResponseParser) {
				
				NetGraphBuilder.buildHTTP1xResponseParser(gm, ccm, toReactor, clearResponse, ackReleaseForResponseParser);
			}
			
		};


		NetGraphBuilder.buildClientGraph(gm, ccm, 
				                             netRespQueue, netRespSize,
				                             clientRequests,responseUnwrapUnits,
											 requestWrapUnits, 
											 clientWriterStages, releaseCount, 
											 netResponseCount, netResponseBlob, factory, writeBufferMultiplier);

		new HTTPClientRequestStage(gm, ccm, input, clientRequests);
		
		//TODO: JUST LIKE GROUPS THESE CAN NOT ACCESS ccm AT THE SAME TIME.
//		Pipe[][] inputs = Pipe.splitPipes(2, input);
//		Pipe[][] requests = Pipe.splitPipes(2, clientRequests);
//				
//		
//		new HTTPClientRequestStage(gm, ccm, inputs[0], requests[0]);
//		new HTTPClientRequestStage(gm, ccm, inputs[1], requests[1]);
//				
		
		return toReactor;
	}

	
	
	
	
}
