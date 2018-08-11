package com.ociweb.pronghorn.network.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.OAuth2BearerExtractor;
import com.ociweb.pronghorn.network.OAuth2BearerUtil;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.TwitterStreamControlSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.parse.JSONParser;

/**
 * _no-docs_
 */
public class RequestTwitterQueryStreamStage extends PronghornStage {

	private static final int PAYLOAD_INDEX_OFFSET = 1;

	private static final Logger logger = LoggerFactory.getLogger(RequestTwitterQueryStreamStage.class);
	
	private final Pipe<TwitterStreamControlSchema>[] streamControlPipe;
	private final Pipe<ClientHTTPRequestSchema> httpRequest;
	private OAuth2BearerExtractor bearerVisitor;
	
	private final int bearerRequestResponseId;
	
	private final int[] queryResponseIds;
	private final String[] queryStrings;
	private long[] queryLastId;
	private short[] cyclesToWait;
	
	
	private byte[] queryRoot;
	private byte[] queryType;
	private byte[] sincePrefix;
	private byte[] countPrefix;
	
	private final int twitterBearerPort;
	private final int twitterQueryPort;
	private final String twitterQueryHost;
	private final String twitterBearerHost;
	
	private final String consumerKey; 
	private final String consumerSecret;
	private final Pipe<NetResponseSchema> newBearerPipe;
	private final int maxCount;
	private int inFlightTimeoutCounter = 0; //when above zero we must not send any more requests
	private final int inFlightTimeoutCycles = 10;//will wait this many run calls before attempting call again.
	
	//NOTE: instead of streaming which leaves the socket open with a few requests comming in slowly we will
	//      connect once every  minute and request the next block to process.
	//      * we get all the data at once for optmized batching
	//      * we can eliminate the need for user auth
	//      * we can add many more than 400 keywords.
	//      * updates are slowed for 400 to once every 40 minutes.
	
	
	//https://api.twitter.com/1.1/search/tweets.json?q=%40twitterapi since:2015-12-21
	//                                                 "java",""c++" since:2017-03-20
	
	// https://dev.twitter.com/rest/public/search
	
	public RequestTwitterQueryStreamStage(
			GraphManager gm,			
			String consumerKey, 
			String consumerSecret, 
			
			int maxCount,
			
			int[] queryResponseIds,
			String[] queryStrings,
			
			int bearerRequestResponseId, 			
			
			Pipe<TwitterStreamControlSchema>[] streamControlPipe, //Bad bearer detected or bad 
			Pipe<NetResponseSchema> newBearerPipe, //new Bearer arrives here
			
			Pipe<ClientHTTPRequestSchema> httpRequest //send out request for Bearer or Query request
			
			) {
		
		super(gm, join(streamControlPipe, newBearerPipe), httpRequest);
		
		this.streamControlPipe = streamControlPipe;
		this.httpRequest = httpRequest;
	    this.bearerRequestResponseId = bearerRequestResponseId;
	    
	    this.queryResponseIds= queryResponseIds;
	    this.queryStrings = queryStrings;
	    
	    
	    this.twitterBearerPort = 443;
	    this.twitterBearerHost = "api.twitter.com"; //NOTE: could be a pipe constant...
	    this.twitterQueryPort  = 443;
	    this.twitterQueryHost  = "api.twitter.com"; //NOTE: could be a pipe constant...
	    
	    this.consumerKey = consumerKey;
	    this.consumerSecret = consumerSecret;
		this.newBearerPipe = newBearerPipe;
		
		this.maxCount = maxCount;
		
		//We are using App Auth, Not user auth so we get 450 reqeusts per 15 min window
		//
		int msBetweenCalls = (15*60*1000)/450;
		assert(2000 == msBetweenCalls);
		//Schedules may go slower than this rate upon occasion but they are guaranteed not to go faster.
		long nsRate = msBetweenCalls*1_000_000;
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, nsRate, this);

		
	}
	
	public static RequestTwitterQueryStreamStage newInstance(GraphManager gm,
										String consumerKey, String consumerSecret,
										int maxCount,
										int[] queryResponseIds, 
										String[] queryStrings,
										int bearerRequestResponseId, 			
										Pipe<TwitterStreamControlSchema>[] streamControlPipe, //Bad bearer detected or bad data
										Pipe<NetResponseSchema> newBearerPipe, //new Bearer arrives here
										Pipe<ClientHTTPRequestSchema> httpRequest //send out request for Bearer or Query request
			) {
		
		return new RequestTwitterQueryStreamStage(gm, consumerKey, consumerSecret,
												  maxCount, queryResponseIds, queryStrings,
				                                  bearerRequestResponseId,
												  streamControlPipe, newBearerPipe, httpRequest);
	}

	@Override
	public void startup() {
		queryRoot = "1.1/search/tweets.json?q=".getBytes();
		queryType = "&result_type=recent".getBytes();
		sincePrefix = "&since_id=".getBytes();
		countPrefix = "&count=".getBytes();
		
		cyclesToWait = new short[queryStrings.length];
		queryLastId = new long[queryStrings.length];
		//needed to read the bearer when it arrives.
		bearerVisitor = new OAuth2BearerExtractor();
		OAuth2BearerUtil.bearerRequest(httpRequest, consumerKey, consumerSecret, twitterBearerHost, twitterBearerPort, bearerRequestResponseId);				
	}

    private int lastPosition = 0;
	
	@Override
	public void run() {
		consumeAnyNewBearer();	
		consumeControlMessages();
		
		if (inFlightTimeoutCounter > 0) {
			inFlightTimeoutCounter--;
			
//			if (inFlightTimeoutCounter==0) {
//				//this is not an error
//				logger.trace("timeout detected, now sending last query again... Can be for low volume query or slow server....");
//			}
			
			return;//call again later
		}
		
		
		//we know our max call rate
		//we know the list of queries
		//find the oldest out of date and run it
		//latency will go up with more values
		
		if (bearerVisitor.hasBearer() ) {
			
			CharSequence headers = "Authorization: Bearer "+bearerVisitor.getBearer()+"\r\n";
					
					
			///////////////////////////////////////////
			//pick the one with the smallest post Id.
			//keeps all our queries balanced
			///////////////////////////////////////////
			int targetIdx = -1; 
			
			int k = queryLastId.length;
			while (--k >= 0) {
			
				if (--lastPosition < 0) {
					lastPosition = queryLastId.length-1;
				}
							
				if (cyclesToWait[lastPosition] > 0) {
					cyclesToWait[lastPosition]--;
				}			
				
				if (cyclesToWait[lastPosition] <= 0) {
					targetIdx = lastPosition;
					break;
				}
				
			}
			////////////////////////////////////////////
						
			if (-1!=targetIdx && PipeWriter.tryWriteFragment(httpRequest, ClientHTTPRequestSchema.MSG_HTTPGET_100)) {
				
				//logger.info("wrote out request for query, all is ready...");

				//if we do not recieve a finished block, do not call again for this many cycles
				inFlightTimeoutCounter = inFlightTimeoutCycles;
				assert(queryResponseIds[targetIdx]>=0);
				PipeWriter.writeInt(httpRequest, 
						            ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_DESTINATION_11,
						            queryResponseIds[targetIdx]);				
				
				
				PipeWriter.writeInt(httpRequest, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_SESSION_10, 0);
				PipeWriter.writeInt(httpRequest, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PORT_1, twitterQueryPort);
				PipeWriter.writeUTF8(httpRequest, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2, twitterQueryHost);
				
				DataOutputBlobWriter<ClientHTTPRequestSchema> stream = PipeWriter.outputStream(httpRequest);
				DataOutputBlobWriter.openField(stream);
				stream.write(queryRoot);
				stream.append(queryStrings[targetIdx]);				
				if (queryLastId[targetIdx]!=0) {
					stream.write(sincePrefix);
					Appendables.appendValue(stream, queryLastId[targetIdx]);
				} else {
					stream.write(queryType);					
				}
				stream.write(countPrefix);
				Appendables.appendValue(stream, maxCount);		
				
				//stream.debugAsUTF8();
				
				DataOutputBlobWriter.closeHighLevelField(stream, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3);
						
				
				PipeWriter.writeUTF8(httpRequest, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HEADERS_7, headers);
						
				PipeWriter.publishWrites(httpRequest);
				
				
			}
		}
	}

	private void consumeControlMessages() {
		int i = streamControlPipe.length;
		while (--i>=0) {
			
			Pipe<TwitterStreamControlSchema> localPipe = streamControlPipe[i];
			
			while (PipeReader.tryReadFragment(localPipe)) {

			    int msgIdx = PipeReader.getMsgIdx(localPipe);
			    switch(msgIdx) {
			        case TwitterStreamControlSchema.MSG_RECONNECT_100:
			        	logger.info("**************  got now reconnect request, requesting new bearer");
			        	bearerVisitor.reset();
			        	//now clear for next query, can run once we get the new bearer, old call was abandoned.
			        	inFlightTimeoutCounter = 0;
			        	OAuth2BearerUtil.bearerRequest(httpRequest, consumerKey, consumerSecret, twitterBearerHost, twitterBearerPort, bearerRequestResponseId);		        	
					break;
			        case TwitterStreamControlSchema.MSG_FINISHEDBLOCK_101:			        	

			        	//now clear for next query
			        	inFlightTimeoutCounter = 0;
			        				        	
			        	long postId = PipeReader.readLong(localPipe,TwitterStreamControlSchema.MSG_FINISHEDBLOCK_101_FIELD_MAXPOSTID_31 );
			        	long prevId = queryLastId[i];
			        	
			        	//System.err.println("consume finish block new: "+postId+" old "+prevId);
			        	
			        	if (postId>prevId) {
			        		queryLastId[i] = postId;
			        	} else if ((postId>0) && (postId==prevId) ) {
			        		//back off per query so we can go do the others while we wait...
			        		//wait for 2 passes over all the queries
			        		cyclesToWait[i] = (short)Math.min(Short.MAX_VALUE, 
			        				                          Math.max(10, cyclesToWait.length*2));
			        		
			        	//	logger.info("must wait for cycles "+cyclesToWait[i]+" at "+postId);
			        		
			        	}
					break;					
			        case -1:
			           requestShutdown();
			        break;
			    }
			    PipeReader.releaseReadLock(localPipe);
			}
		}
	}
	
	private void consumeAnyNewBearer() {
		while (PipeReader.tryReadFragment(newBearerPipe)) {
			
			//logger.info("consumed new bearer");
			
			if (PipeReader.getMsgIdx(newBearerPipe)==NetResponseSchema.MSG_RESPONSE_101) {
				
				long con = PipeReader.readLong(newBearerPipe, NetResponseSchema.MSG_RESPONSE_101_FIELD_CONNECTIONID_1);
				int flags = PipeReader.readInt(newBearerPipe, NetResponseSchema.MSG_RESPONSE_101_FIELD_CONTEXTFLAGS_5);
				
				DataInputBlobReader<NetResponseSchema> stream = PipeReader.inputStream(newBearerPipe,  NetResponseSchema.MSG_RESPONSE_101_FIELD_PAYLOAD_3);		

				//System.out.println(stream.getClass());
						
				
				
				short statusCode =stream.readShort();
				
				if (200!=statusCode) {
					logger.info("error got code:{}",statusCode);
					PipeReader.releaseReadLock(newBearerPipe);
					continue;
				}
				
				//skip over all the headers, no need to read them at this time
				DataInputBlobReader.position(stream, stream.readFromEndLastInt(PAYLOAD_INDEX_OFFSET));

				TrieParserReader jsonReader = JSONParser.newReader();
				JSONParser.parse(stream, jsonReader, bearerVisitor);
			} else {
				System.out.println("unknown "+PipeReader.getMsgIdx(newBearerPipe));
			}			
			PipeReader.releaseReadLock(newBearerPipe);
			
			
		}
	}
	
}
