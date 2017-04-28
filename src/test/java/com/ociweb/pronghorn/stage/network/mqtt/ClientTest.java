package com.ociweb.pronghorn.stage.network.mqtt;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.network.mqtt.MQTTClientGraphBuilder;
import com.ociweb.pronghorn.network.schema.MQTTClientRequestSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class ClientTest {

    
    @Test
    public void simpleClientTest() {
        
       GraphManager gm = new GraphManager();
       
		final boolean isTLS = false;
		
		int maxInFlight = 10;
		int maximumLenghOfVariableLengthFields = 4096;
		
		Pipe<MQTTClientRequestSchema> clientRequest = MQTTClientRequestSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
		clientRequest.initBuffers();
		
		Pipe<MQTTClientResponseSchema> clientResponse = MQTTClientResponseSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
		
		MQTTClientGraphBuilder.buildMQTTClientGraph(gm, isTLS, maxInFlight, maximumLenghOfVariableLengthFields, clientRequest, clientResponse);       
       
        ConsoleJSONDumpStage.newInstance(gm, clientResponse);
       
        MonitorConsoleStage.attach(gm);
        
        NonThreadScheduler scheduler = new NonThreadScheduler(gm);
       
        scheduler.startup();
       
       int j  = 100; //build up some Ids
       while (--j>=0) {
           scheduler.run();
       }
       
       CharSequence fieldHost = "127.0.0.1";
       int fieldPort = isTLS ? 8883 : 1883;
       
       boolean okHost = PipeWriter.tryWriteFragment(clientRequest, MQTTClientRequestSchema.MSG_BROKERCONFIG_100);
       assertTrue(okHost);
       
	   PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_BROKERCONFIG_100_FIELD_HOST_26, fieldHost);
	   PipeWriter.writeInt(clientRequest,MQTTClientRequestSchema.MSG_BROKERCONFIG_100_FIELD_PORT_27, fieldPort);
	   PipeWriter.publishWrites(clientRequest);
	  
       
       
       int fieldKeepAliveSec = 4;
       int fieldFlags = 0;
       
       CharSequence fieldClientId = "testClient";
       CharSequence fieldWillTopic = "";
       ByteBuffer fieldWillPayload = ByteBuffer.allocate(0);
       CharSequence fieldUser = "";
       CharSequence fieldPass = "";

       
        boolean okCon = PipeWriter.tryWriteFragment(clientRequest, MQTTClientRequestSchema.MSG_CONNECT_1);
        assertTrue(okCon);
        
	    PipeWriter.writeInt(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_KEEPALIVESEC_28, fieldKeepAliveSec);
	    PipeWriter.writeInt(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_FLAGS_29, fieldFlags);
	    PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_CLIENTID_30, fieldClientId);
	    PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_WILLTOPIC_31, fieldWillTopic);
	    PipeWriter.writeBytes(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_WILLPAYLOAD_32, fieldWillPayload);
	    PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_USER_33, fieldUser);
	    PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_PASS_34, fieldPass);
	    PipeWriter.publishWrites(clientRequest);

              
              
       //need an instance of broker running somewhere?? mosquitto already running on my dev box.

       // watch at prompt mosquitto_sub -t /# -q 1
       
	    //TODO: check clear flag and what to do
	    //TODO: check for ack back...

	    int fieldQOS = 1;
       
       byte[] payload = ("hello "+fieldQOS).getBytes();
       int payloadIdx = 0;
       int payloadLength = payload.length;
       int payloadMask = Integer.MAX_VALUE;
       
       
       int fieldRetain = 0;
       CharSequence fieldTopic = "/sensors/temprature";      
       
       boolean okPub = PipeWriter.tryWriteFragment(clientRequest, MQTTClientRequestSchema.MSG_PUBLISH_3);
       assertTrue(okPub);
	   PipeWriter.writeInt(clientRequest,MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_QOS_21, fieldQOS);
	   PipeWriter.writeInt(clientRequest,MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_RETAIN_22, fieldRetain);
	   PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_TOPIC_23, fieldTopic);
	   PipeWriter.writeBytes(clientRequest,MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25, payload, payloadIdx, payloadLength);
	   PipeWriter.publishWrites(clientRequest);

              
	   long target = System.currentTimeMillis()+24_000;
       
       while (System.currentTimeMillis()<target) {
           scheduler.run();
           Thread.yield();
       }
       
       scheduler.shutdown();
       
       
    }
    
    
}
