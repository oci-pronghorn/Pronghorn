package com.ociweb.pronghorn.stage.network;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.ClientResponseParserFactory;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.http.HTTPClientRequestStage;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import org.junit.Ignore;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ClientHTTPSPipelineTest {

	
	@Ignore // do not disable it requires interet access to run, we will make a better test soon.
	public void buildPipelineTest() {

		//only build minimum for the pipeline
		
		GraphManager gm = new GraphManager();

		final int inputsCount = 2;
		
		int base2SimultaniousConnections = 1;//TODO: bug, why can we not set this to larger nubmer??
		final int outputsCount = 1;//must be < connections
		int maxPartialResponses = 2;
		final int maxListeners = 1<<base2SimultaniousConnections;

		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 20_000);

		TLSCertificates certs = TLSCertificates.defaultCerts;
		ClientCoordinator ccm = new ClientCoordinator(base2SimultaniousConnections,inputsCount,certs,gm.recordTypeData);

		
		//IntHashTable.setItem(listenerPipeLookup, 42, 0);//put on pipe 0
		
		
		PipeConfig<ClientHTTPRequestSchema> netREquestConfig = new PipeConfig<ClientHTTPRequestSchema>(ClientHTTPRequestSchema.instance, 30,1<<9);
		PipeConfig<NetPayloadSchema> clientNetRequestConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance,4,16000); 
				
		PipeConfig<NetResponseSchema> netResponseConfig = new PipeConfig<NetResponseSchema>(NetResponseSchema.instance, 10, 1<<15); //if this backs up we get an error TODO: fix

		
		//holds new requests
		Pipe<ClientHTTPRequestSchema>[] input = new Pipe[inputsCount];		
		//responses from the server	
		final Pipe<NetResponseSchema>[] toReactor = new Pipe[maxListeners];	
		
		
		int m = maxListeners;
		while (--m>=0) {
			toReactor[m] = new Pipe<NetResponseSchema>(netResponseConfig);
		}
		
		
		int k = inputsCount;
		while (--k>=0) {
            input[k] = new Pipe<ClientHTTPRequestSchema>(netREquestConfig);	
		}	
		
		Pipe<NetPayloadSchema>[] clientRequests = new Pipe[outputsCount];
		int r = outputsCount;
		while (--r>=0) {
			clientRequests[r] = new Pipe<NetPayloadSchema>(clientNetRequestConfig);		
		}
		HTTPClientRequestStage requestStage = new HTTPClientRequestStage(gm, ccm, input, clientRequests);
		
		
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {

			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, 
								    Pipe<NetPayloadSchema>[] clearResponse,
								    Pipe<ReleaseSchema> ackReleaseForResponseParser) {
				
				NetGraphBuilder.buildHTTP1xResponseParser(gm, ccm, toReactor, clearResponse, ackReleaseForResponseParser);
			}
			
		};
		
		NetGraphBuilder.buildClientGraph(gm, ccm, 
				                             10, clientRequests,
				                             2,2,
											 2, 
											 2048, 64, factory);
		
		int i = toReactor.length;
		PipeCleanerStage[] cleaners = new PipeCleanerStage[i];
		while (--i>=0) {
			cleaners[i] = new PipeCleanerStage<>(gm, toReactor[i]); 
		}

		
		

		//GraphManager.exportGraphDotFile(gm, "httpClientPipeline");
		
		//MonitorConsoleStage.attach(gm);
		//GraphManager.enableBatching(gm);
		
		//TODO: why is this scheduler taking so long to stop.
		//StageScheduler scheduler = new FixedThreadsScheduler(gm, 5);
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		
		
		
		scheduler.startup();		


		
		final int MSG_SIZE = 6;
		
		int testSize = 100; 
		
		int requests = testSize;
		
		
		long timeout = System.currentTimeMillis()+(testSize*1500); //reasonable timeout
		
		long start = System.currentTimeMillis();
		int d = 0;
	//	int b = 0;
		while (requests>0 && System.currentTimeMillis()<timeout) {
						
			Pipe<ClientHTTPRequestSchema> pipe = input[0];
			if (PipeWriter.tryWriteFragment(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100)) {
				assert((requests%maxListeners)>=0);
				PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_DESTINATION_11, requests%maxListeners);
				PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PORT_1, 443);
				PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2, "encrypted.google.com");
				PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_SESSION_10,  requests%maxListeners);
				PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3, "/");
				PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HEADERS_7, "");
				PipeWriter.publishWrites(pipe);

				requests--;
			
				d+=MSG_SIZE;
				
				Thread.yield();
			} 
//			else {
//				try {
//					Thread.sleep(1);
//				} catch (InterruptedException e) {
//					break;
//				}
//				if (++b==1000){
//					System.err.println("pipe is backed up, can not make new requests to "+input[0]);
//					System.err.println("requests pipe is "+clientRequests[0]);
//				}
//			}
		}
		

		//count total messages, we know the parser will only send 1 message for each completed event, it does not yet have streaming.

		System.out.println("watching for responses");
		
		int expected = MSG_SIZE*(testSize);
		int count = 0;
		int lastCount = 0;
		do {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				break;
			}
			
			count = doneCount(cleaners);
			
			if (count!=lastCount) {				
				lastCount = count;
				
				if (System.currentTimeMillis()-start>20_000) {				
					System.err.println("pct "+((100f*lastCount)/(float)expected));
				}
			}

		} while (count<expected && System.currentTimeMillis()<timeout);

		long duration = System.currentTimeMillis()-start;
		//59 65 67 63
		
		//74 69 74
		System.out.println("shutdown");
		scheduler.shutdown();
		scheduler.awaitTermination(2, TimeUnit.SECONDS);
			
		System.out.println("duration: "+duration);
		System.out.println("ms per call: "+(duration/(float)testSize));
		
		assertEquals(expected, lastCount);

		
		
		
	}

	private int doneCount(PipeCleanerStage[] cleaners) {
		int count;
		{
		int k = cleaners.length;
		int c=0;
		while (--k>=0) {
			c+=cleaners[k].getTotalSlabCount();
			
		}
		
		count = c;
		}
		return count;
	}
	
}
