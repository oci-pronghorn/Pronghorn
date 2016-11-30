package com.ociweb.pronghorn.stage.network;

import java.io.IOException;
import java.util.Arrays;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ClientHTTPRequestDataGeneratorStage extends PronghornStage {

    private byte[] rawData;
    private int rawDataLength;
    private int[] rawDataSizes;
    
    private int chunkCount;
    private int pos;
    private final Pipe<NetPayloadSchema> output;
    private final int iterations;
    private int count;    
    private final CharSequence[] paths;
    
    private final String verb;
    
    //HTTPRouterStage.newInstance
    public static ClientHTTPRequestDataGeneratorStage newInstance(GraphManager gm, Pipe<NetPayloadSchema> output, int iterations, CharSequence[] paths) {
        return new ClientHTTPRequestDataGeneratorStage(gm, output, iterations, paths);
    }
     
    public ClientHTTPRequestDataGeneratorStage(GraphManager gm, Pipe<NetPayloadSchema> output, int iterations, CharSequence[] paths) {
        super(gm, NONE, output);
        
        this.output = output;
        
        this.iterations = iterations;
        this.count = iterations;
        this.pos = 0;
        this.paths = paths;
        GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER,  this);
            
        verb = "GET "; // .5M msg/sec
        //verb = "HEAD "; //1.3M msg/sec

        
    }
    
    @Override
    public void startup() {
        
        StringBuilder builder = new StringBuilder();
        
        
        rawDataSizes = new int[paths.length];
        
        for(int i = 0; i<rawDataSizes.length; i++) {
            int startPos = builder.length();
            httpRequestBuilder(verb, builder, paths[i]);

            int length = builder.length()-startPos;
            //System.out.println("len "+length);
            if (length > output.maxAvgVarLen) {
                throw new UnsupportedOperationException("expand output blob length it must be larger than "+length+" the value was "+output.maxAvgVarLen);
            }
            rawDataSizes[i]=length;
        }
        
//            System.out.println("TESTRequest\n"+builder);
//            System.out.println("TESTRequest\n"+Arrays.toString(builder.toString().getBytes()));
        
        rawData = builder.toString().getBytes(); //TODO: make conversion without creating string
        rawDataLength = rawData.length;
       
    }
    
    private void httpRequestBuilder(String verb, Appendable target, CharSequence path) {
               
        //request    
        //  GET /hello/myfile HTTP/1.1
        //  Host: 127.0.0.1:8081
        //  Connection: keep-alive
        //  Cache-Control: max-age=0
        //  Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
        //  Upgrade-Insecure-Requests: 1
        //  User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/47.0.2526.106 Chrome/47.0.2526.106 Safari/537.36
        //  DNT: 1
        //  Accept-Encoding: gzip, deflate, sdch
        //  Accept-Language: en-US,en;q=0.8
        
        //connection must apper with upgrade and upgrade should appear in first couple
//        Upgrade: websocket   h2c    -- what are the valid values?   HTTP/2.0, SHTTP/1.3, IRC/6.9, RTA/x11 protocols the server may upgrade to
//        Connection: Upgrade -- what are the valid values?  keep-alive
        
        try {
            target.append(verb);
            target.append(path);
            if (' '!=path.charAt(path.length()-1)) {
                target.append(' ');
            }
            target.append("HTTP/1.1\r\n"); //MUST have white space before the path or this does not work.
            target.append("Host: 127.0.0.1:8081\r\n");
            target.append("Connection: keep-alive\r\n");
            target.append("Cache-Control: max-age=0\r\n");
           // target.append("Upgrade: WAT\r\n");
            target.append("Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\n");
            target.append("User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/47.0.2526.106 Chrome/47.0.2526.106 Safari/537.36\r\n");
              
            target.append("Upgrade-Insecure-Requests: 1\n");
            
            target.append("DNT: 1\r\n");
            target.append("Accept-Language: en-US,en;q=0.8\r\n");
            target.append("Accept-Encoding: gzip, deflate, sdch\r\n");
            target.append("\r\n");//Official end-of-header
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    long realCount = 0;
    
    @Override
    public void run() {        
        
        while (pos < rawDataLength && Pipe.hasRoomForWrite(output)) {
                        
            int length = rawDataSizes[chunkCount++];
            int offset = pos;
            
            writeTestRecord(length, offset);
            
            realCount++;
            
            pos += length;
            
            if (pos>=rawDataLength) {
                if (--count >=0 ) {
                    pos = 0;
                    chunkCount = 0;
                } else {
                    Pipe.publishAllBatchedWrites(output);
                    requestShutdown();
                    System.out.println("Done sending data.");
                }
            }
        }

    }

    private void writeTestRecord(int length, int offset) {
        final int size = Pipe.addMsgIdx(output, NetPayloadSchema.MSG_PLAIN_210);
        Pipe.addLongValue(0, output); //channel
        Pipe.addLongValue(Pipe.getWorkingTailPosition(output), output);
        Pipe.addByteArray(rawData, offset, length, output);
        Pipe.confirmLowLevelWrite(output, size);
        Pipe.publishWrites(output);            
    }
    
   
    @Override
    public void shutdown() {
        
        System.out.println("**** ClientHTTPRequest DataGenerator finished sending  expectedCount:"+(iterations*(long)rawDataSizes.length)+" actualCount:"+realCount);
        Pipe.spinBlockForRoom(output, Pipe.EOF_SIZE);
        Pipe.publishEOF(output);
        
    }


    
}
