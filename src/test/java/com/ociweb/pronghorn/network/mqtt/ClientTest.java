package com.ociweb.pronghorn.network.mqtt;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.network.schema.MQTTConnectionInSchema;
import com.ociweb.pronghorn.network.schema.MQTTConnectionOutSchema;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;

public class ClientTest {

    
    @Ignore
    public void simpleClientTest() {
        
       GraphManager gm = new GraphManager();
       
       
       Pipe<MQTTIdRangeSchema> idGenIn = MQTTIdRangeSchema.instance.newPipe(100,0);
       Pipe<MQTTConnectionOutSchema> fromBroker = MQTTConnectionOutSchema.instance.newPipe(30, 1024);
       Pipe<MQTTConnectionInSchema> toBroker = MQTTConnectionInSchema.instance.newPipe(30, 1024);
       int ttlSec = 10;
       MQTTAPIStage api = new MQTTAPIStage(gm, idGenIn, fromBroker, toBroker, ttlSec);
       
       Pipe<MQTTIdRangeSchema> idGenOut= MQTTIdRangeSchema.instance.newPipe(100,0);
       String rate = "0";
       int inFlight =  10;
       boolean secure = false;
       int port= 1883;
       
       new MQTTConnectionStage(gm, toBroker, fromBroker, idGenOut, rate, inFlight, ttlSec, secure, port); 
       
       new IdGenStage(gm, idGenOut, idGenIn, rate);
       
       NonThreadScheduler scheduler = new NonThreadScheduler(gm);
       
       scheduler.startup();
       
       int j  = 100; //build up some Ids
       while (--j>=0) {
           scheduler.run();
       }
       
       String url = "127.0.0.1";
              
       int conFlags = 0;
       
       byte[] willTopic = null;
       int willTopicIdx = 0;
       int willTopicLength = 0;
       int willTopicMask = 0;
       
       byte[] willMessageBytes = null;
       int willMessageBytesIdx = 0;
       int willMessageBytesLength = 0;
       int willMessageBytesMask = 0;
       byte[] username = "".getBytes();
       byte[] passwordBytes = "".getBytes();
    
       boolean requested = api.requestConnect(url, conFlags, 
                          willTopic, willTopicIdx, willTopicLength, willTopicMask, willMessageBytes, willMessageBytesIdx, willMessageBytesLength, willMessageBytesMask, 
                          username, passwordBytes);
       
       assertTrue(requested);
       
              
       //need an instance of broker running somewhere?? mosquitto already running on my dev box.
       // watch at prompt mosquitto_sub -t sensors/temperature -q 1
       // watch at prompt mosquitto_sub -t /# -q 1
       
       
       
       byte[] topic = "sensors/temprature".getBytes();
       int topicIdx =0;
       int topicLength = topic.length;
       int topicMask = Integer.MAX_VALUE;
       
       int qualityOfService = 1;       
       int retain = 0;
       
       byte[] payload = ("hello "+qualityOfService).getBytes();
       int payloadIdx = 0;
       int payloadLength = payload.length;
       int payloadMask = Integer.MAX_VALUE;
       
       int packetId = api.requestPublish(topic, topicIdx, topicLength, topicMask, qualityOfService, retain, payload, payloadIdx, payloadLength, payloadMask);
       
       assertFalse(packetId==-1);
       
       int i  = 100;
       while (--i>=0) {
           scheduler.run();
       }
       
       
    }
    
    
}
