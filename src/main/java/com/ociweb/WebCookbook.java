package com.ociweb;

import java.io.File;

import com.ociweb.json.JSONAccumRule;
import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.json.JSONType;
import com.ociweb.json.decode.JSONExtractor;
import com.ociweb.pronghorn.network.DummyRestStage;
import com.ociweb.pronghorn.network.HTTPServerConfig;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.ServerFactory;
import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.http.ModuleConfig;
import com.ociweb.pronghorn.network.http.RouterStageConfig;
import com.ociweb.pronghorn.network.module.FileReadModuleStage;
import com.ociweb.pronghorn.network.module.ResourceModuleStage;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.blocking.BlockingSupportStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.util.MainArgs;

public class WebCookbook  {


	public static void main(String[] args) {
		
		
		String home = System.getenv().get("HOME");
		String filesPath = MainArgs.getOptArg("files", "-f", args, (null==home?"~":home)+"/www");

		GraphManager gm = new GraphManager();		
		populateGraph(gm, filesPath);		
		gm.enableTelemetry(8089);		
		StageScheduler.defaultScheduler(gm).startup();
		
	}


	private static void populateGraph(GraphManager gm, String filesPath) {
		
		HTTPServerConfig serverConfig = NetGraphBuilder.serverConfig(8080, gm);
		
		//show all these
		serverConfig.setDecryptionUnitsPerTrack(2);
		serverConfig.setConcurrentChannelsPerDecryptUnit(8);
		serverConfig.setEncryptionUnitsPerTrack(2);
		serverConfig.useInsecureServer();//TODO: turn this off later...
		
		serverConfig.logTraffic(false);//we are sending back images so do not log the responses.
		
		NetGraphBuilder.buildServerGraph(gm, serverConfig.buildServerCoordinator(), new ServerFactory() {
		
			@Override
			public void buildServer(GraphManager gm, 
									ServerCoordinator coordinator,
									Pipe<ReleaseSchema>[] releaseAfterParse, 
									Pipe<NetPayloadSchema>[] receivedFromNet,
									Pipe<NetPayloadSchema>[] sendingToNet) {
								
				NetGraphBuilder.buildHTTPStages(gm, coordinator, buildModules(filesPath), 
										        releaseAfterParse, receivedFromNet, sendingToNet);
			}
		});
		
	}

	private static ModuleConfig buildModules(String filesPath) {
		
		return new ModuleConfig() {

			@Override
			public int moduleCount() {
				return 6;
			}

			@Override
			public Pipe<ServerResponseSchema>[] registerModule(
					int moduleInstance, GraphManager graphManager,
					RouterStageConfig routerConfig, 
					Pipe<HTTPRequestSchema>[] inputPipes) {
				
				switch(moduleInstance) {
				
					case 0: //files served from resources
						{
						//if we like we can create one module for each input pipe or as we do here
					    //create one module to consume all the pipes and produce results.
						int maximumLenghOfVariableLengthFields = 1<<16;//TODO: max file size based on files??
						Pipe<ServerResponseSchema>[] response = Pipe.buildPipes(inputPipes.length, 
								 ServerResponseSchema.instance.newPipeConfig(2, maximumLenghOfVariableLengthFields));
								
						ResourceModuleStage.newInstance(graphManager, 
								inputPipes, 
								response, 
								routerConfig.httpSpec(),
								"site/","index.html"); //good example since telemetry is in resources...
						
						//http://10.10.10.105:8080/resource/reqPerSec.png
						//http://172.16.10.221:8080/resource/reqPerSec.png
						routerConfig.registerCompositeRoute()
						            .path("/resource/${path}") //multiple paths can be added here
						            .routeId();
						
						return response;
						}
					case 1: //files served from drive folder
						{
							
						PipeConfig<ServerResponseSchema> config
									= ServerResponseSchema.instance.newPipeConfig(32, 1<<10);
							
						Pipe<ServerResponseSchema>[] response = Pipe.buildPipes(inputPipes.length, config);
						
						int instances = inputPipes.length;
						
						Pipe<ServerResponseSchema>[] responses = new Pipe[instances];
						
						File rootPath = new File(filesPath);
										
						//creates 1 file responder for every input, we could have just built 1 and had them share
						int i = instances;
						while (--i>=0) {
							responses[i] = new Pipe<ServerResponseSchema>(config);
							FileReadModuleStage.newInstance(graphManager, inputPipes[i],
							responses[i], 
							routerConfig.httpSpec(), 
							rootPath);					
						}
							
						//http://10.10.10.105:8080/file/GLLatency2.png
						//http://172.16.10.221:8080/file/GLLatency2.png
						routerConfig.registerCompositeRoute().path("/file/${path}").routeId();
						
						return responses;
						}					
					case 2: //custom REST call
						{
						Pipe<ServerResponseSchema>[] responses = Pipe.buildPipes(inputPipes.length, 
								 ServerResponseSchema.instance.newPipeConfig(2, 1<<9));
							
						//TODO: replace this with custom rest call?
						DummyRestStage.newInstance(
								graphManager, inputPipes, responses, 
								routerConfig.httpSpec()
								);
												
						JSONExtractorCompleted extractor =
								new JSONExtractor()
								 .begin()
								 .element(JSONType.TypeInteger, false, JSONAccumRule.First)
								 .asField("root.value","someValue")								 
								 .finish();
						
						routerConfig.registerCompositeRoute(extractor)
				            .path("/rest/myCall?a=#{aVal}&text=${textVal}") //multiple paths can be added here
				            .routeId();
						
						return responses;
						}
					case 3:	
						{
						
						int tracks = inputPipes.length;
						Pipe<ServerResponseSchema>[] responses = Pipe.buildPipes(
									tracks, 
									ServerResponseSchema.instance.newPipeConfig(2, 1<<9));

						
						Pipe<ClientHTTPRequestSchema>[] clientRequests = Pipe.buildPipes(
									tracks, 
									ClientHTTPRequestSchema.instance.newPipeConfig(2, 1<<9));

						Pipe<NetResponseSchema>[] clientResponses = Pipe.buildPipes(
									tracks, 
									NetResponseSchema.instance.newPipeConfig(2, 1<<9));
								
						Pipe<ConnectionData>[] connectionData = Pipe.buildPipes(
								tracks, 
								ConnectionData.instance.newPipeConfig(2, 1<<9));
						
						//TODO: turn this on later..
						TLSCertificates tlsCertificates = null;//TLSCertificates.defaultCerts;
						int connectionsInBits = 3;
						int maxPartialResponses = 4;					
						int clientRequestCount=5; 
						int clientRequestSize=200;
																		
						RequestToBackEnd.newInstance(graphManager, inputPipes, connectionData, clientRequests);

						NetGraphBuilder.buildHTTPClientGraph(graphManager, 
															clientResponses,
															clientRequests,
															maxPartialResponses, connectionsInBits,
															clientRequestCount,
															clientRequestSize,
															tlsCertificates);
						
						ResponseFromBackEnd.newInstance(graphManager, clientResponses, connectionData, responses);
								
						routerConfig.registerCompositeRoute()
				            .path("/rest/myCall?${argsVal}") //multiple paths can be added here
				            .routeId();
						
						return responses;
						}
					case 4:
						{
							
						final int blockers = inputPipes.length;
						
						Pipe<ServerResponseSchema>[] responses = Pipe.buildPipes(inputPipes.length, 
								 ServerResponseSchema.instance.newPipeConfig(2, 1<<9));
						
						long timeoutNS = 10_000_000_000L;//10sec
						
								
						for(int i = 0; i<blockers; i++) {
							//one blocking stage for each of the tracks
							new BlockingSupportStage<HTTPRequestSchema,ServerResponseSchema,ServerResponseSchema>(graphManager, 
									inputPipes[i], responses[i], responses[i], 
									timeoutNS, 
									(t)->{return ((int)(long) Pipe.peekInt(t, HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_CHANNELID_21))%blockers;}, 
									new DBCaller(), new DBCaller(), new DBCaller()); //TODO: is caller right? DB problems
						}
						
						// http://172.16.10.221:8080/person/add?id=333&name=nathan
						// http://172.16.10.221:8080/proxy/list
						
						// http://10.10.10.105:8080/person/add?id=333&name=nathan
						// http://10.10.10.105:8080/proxy/list
						
						routerConfig.registerCompositeRoute()
						    .path("/proxy/list") //multiple paths can be added here
				            .path("/person/add?id=#{id}&name=${name}") //multiple paths can be added here
				            .defaultInteger("id", Integer.MIN_VALUE)
							.defaultText("name", "")
							.associatedObject("id", WebFields.id)
							.associatedObject("name", WebFields.name)
				            .routeId();
						
						return responses;
						}
					case 5:
						{
							Pipe<ServerResponseSchema>[] responses = Pipe.buildPipes(inputPipes.length, 
									 ServerResponseSchema.instance.newPipeConfig(2, 1<<9));
								
							DummyRestStage.newInstance(
									graphManager, inputPipes, responses, 
									routerConfig.httpSpec()
									);
									
							routerConfig.registerCompositeRoute()
					            .path("/dummy/${textVal}") //multiple paths can be added here
					            .routeId();
							
							return responses;
							}
					default:
						throw new UnsupportedOperationException();

				}				
			}			
		};
	} 
}
