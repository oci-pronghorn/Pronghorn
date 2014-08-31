package com.ociweb.jfast.mux;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.junit.Test;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTInputSocketChannel;

public class MuxTest {

    
    @Test
    public void loadFile() {
        
        byte[] catBytes = buildRawCatalogData(new ClientConfig());
        
        URL sourceData = getClass().getResource("/performance/complex30000.dat");
        File sourceDataFile = new File(sourceData.getFile().replace("%20", " "));
       
        
        //Note: need better aproach
        //need to have these bytes read in while parseing the preamble. the 
        //FASTInput objects (n of these) need to be given the new location in the array to read from
        //TODO  B, this is sequential and use FASTInput that can use NIO changes that are made up of multiple sources.
        //TODO B, must compute max parallel in config, Minimum readers is 1 what is the maximum?
        //Must be less than cores or acceptable threads.
        //Must be limited by the maximum parallel structure implied by the catalog. need new compute in catalog for this.
        //Any reset message is isolated and does not limit, any messages that share dictionary fields must be done in one decoder.
        //once decoders read the fields the ring buffers must reconstruct the messages in order.
        //the decode must write to ring buffer the ids of each ring buffer used then this meta buffer is read to read in the right order by the conusmer.
        //TODO: B, need fake ring buffer that merges others, Zip FlatMap?
        
        //FASTInputByteBuffer input = buildInputForTestingByteBuffer(sourceDataFile);
        
        
        FASTInputSocketChannel targetChannel = null;
        
        WritableByteChannel target = null;
        
        
        
        int loops = 4;
        
        long totalTestBytes = sourceDataFile.length();
        System.err.println(totalTestBytes);
        long begin = System.nanoTime();
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(sourceDataFile, "r");
            randomAccessFile.seek(0);
            
            begin = System.nanoTime();
            int preamble;
            
            FileChannel fc = randomAccessFile.getChannel();
            MappedByteBuffer mem = fc.map(FileChannel.MapMode.READ_ONLY, 0, totalTestBytes);
                 
           int j = loops;
           while (--j>=0) {
            
                mem.position(0);
                
                do { 
                    //this should be the size of this block
                    preamble = mem.get();
                    preamble |= mem.get()<<8;
                    preamble |= mem.get()<<16;
                    preamble |= mem.get()<<24;
                    int newPos = mem.position()+preamble;
                    //peek ahead at the templateId which will be after the pmap
                    
                    int pmap =  mem.get();
                    
                    int templateId = readIntegerUnsigned(mem);
                    
                    //all the 2's will go to another
                    //all the 1's can round robin the shortest
                    
                    
                   // int templateId = mem.get(); //this is encoded however!
                    
                    
               //     System.err.println(templateId);
                    
                   
                    
//                    WritableByteChannel target = null;
//                    fc.transferTo(mem.position(), preamble, target);//does zero copy when it can
                    
                    
                    mem.position(newPos);//now at start of next block or end of file
                    
                    
                    
                
                   // System.err.println(mem.remaining());
                }while (mem.remaining()>0);
                
           }         
            
            fc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        long duration = System.nanoTime()-begin;
        
        long bytes = totalTestBytes*loops;
        
        //need MB per second
        double bytesPerSecond = ( 1000000000d * bytes)/(double)duration;
        System.err.println((bytesPerSecond/ (1<<20)) + " MBps");
        System.err.println(duration+" ns");
        
    }
    
    
    public static int readIntegerUnsigned(MappedByteBuffer mem) {//Invoked 100's of millions of times, must be tight.
        byte v = mem.get();
        return (v < 0) ? (v & 0x7F) : readIntegerUnsignedTail(v,mem);

    }

    //TODO: C, add overflow flag to support optional int that is outside 32 bits. Without this we dont quite match the spec.
    
    //recursive use of the stack turns out to be a good way to unroll this loop.
    private static int readIntegerUnsignedTail(int a, MappedByteBuffer mem) {
        byte v = mem.get();
        return (v<0) ? (a << 7) | (v & 0x7F) : readIntegerUnsignedTail((a<<7)|v,mem);
    }
    
    private FASTInputByteBuffer buildInputForTestingByteBuffer(File sourceDataFile) {
        long totalTestBytes = sourceDataFile.length();
        FASTInputByteBuffer fastInput = null;
        try {
            FileChannel fc = new RandomAccessFile(sourceDataFile, "r").getChannel();
            MappedByteBuffer mem = fc.map(FileChannel.MapMode.READ_ONLY, 0, totalTestBytes);
            fastInput = new FASTInputByteBuffer(mem);
            fc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fastInput;
    }
    
    
    public static byte[] buildRawCatalogData(ClientConfig clientConfig) {
        //this example uses the preamble feature
        clientConfig.setPreableBytes((short)4);

        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, "/performance/example.xml", clientConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue("Catalog must be built.", catalogBuffer.size() > 0);

        byte[] catalogByteArray = catalogBuffer.toByteArray();
        return catalogByteArray;
    }
    
}
