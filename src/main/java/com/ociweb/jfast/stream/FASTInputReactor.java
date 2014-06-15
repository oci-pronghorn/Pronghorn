//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.InputBlockagePolicy;
import com.ociweb.jfast.primitive.PrimitiveReader;

/*
 * Implementations of read can use this object
 * to pull the most recent parsed values of any available fields.
 * Even those in outer groups may be read however values appearing in the template
 * after the <groupId> will not have been read yet and are not available.
 * If those values are needed wait until this method is called with the
 * desired surrounding <groupId>.
 * 
 * Supports dynamic modification of the templates including:  
 * 		Field compression/operation type changes.
 * 	    Field order changes within a group.
 *      Mandatory/Optional field designation.
 *      Pulling up fields from group to the surrounding group.
 *      Pushing down fields from group to the internal group.
 * 
 * In some cases after modification the data will no longer be available
 * and unexpected results can occur.  Caution must be used whenever pulling up
 * or pushing down fields as it probably changes the meaning of the data. 
 * 
 */
public final class FASTInputReactor {

    /**
     * Read up to the end of the next sequence or message (eg. a repeating
     * group)
     * 
     * Rules for making client compatible changes to templates. - Field can be
     * demoted to more general common value before the group. - Field can be
     * promoted to more specific value inside sequence - Field order inside
     * group can change but can not cross sequence boundary. - Group boundaries
     * can be added or removed.
     * 
     * Note nested sequence will stop once for each of the sequences therefore
     * at the bottom hasMore may not have any new data but is only done as a
     * notification that the loop has completed.
     * 
     * @return
     */
    
    private final FASTDecoder decoder; 
    private final PrimitiveReader reader;//the reader is non-blocking but awkward to use directly.
    private final FASTListener listener;
    
    //TODO: single execution service must be used for all and passed in.
    //TODO: reactor will add its runnable to to the single service and remove upon dispose.
    //TODO: runnable loops as long as there is data, no data then wait until feed in data.
    //TODO: if the feed stops in the middle of the data must wait that thread till it gets in.
    
    public FASTInputReactor(FASTDecoder decoder, PrimitiveReader reader) {
        this.decoder=decoder;
        this.reader=reader;
        this.listener = new FASTListener(){

            @Override
            public void fragment(int templateId, FASTRingBuffer buffer) {
            }

            @Override
            public void fragment() {
            }};
        
    }

    
    public FASTInputReactor(FASTDecoder decoder, PrimitiveReader reader, FASTListener listener) {
        this.decoder=decoder;
        this.reader=reader;
        this.listener = listener;
    }
    
    public void start(final ThreadPoolExecutor executorService, PrimitiveReader reader) {
        
        PrimitiveReader.setInputPolicy(new InputBlockagePolicy() {
            Object lock = new Object();

            @Override
            public void detectedInputBlockage(int need, FASTInput input) {
                //TODO: A, create these extra threads on startup and pause them until this moment, this prevents creation and gc at runtime
                synchronized(lock) {
                    executorService.setMaximumPoolSize(executorService.getMaximumPoolSize()+1);
                }                
            }
             
            @Override
            public void resolvedInputBlockage(FASTInput input) {
                synchronized(lock) {
                    executorService.setMaximumPoolSize(executorService.getMaximumPoolSize()-1);
                }
            }
            
        });
        
        final Runnable run = new Runnable() {

            @Override
            public void run() {
                int f;
                int c = 0xFFF;
                while ((f=pump2())>=0 && --c>=0) {
                    
                }
                
                if (f>=0) {
                  //  System.err.println("pump");
                    executorService.execute(this);
                } else {
                    executorService.shutdown();
                }
            }
            
        };        
        executorService.execute(run);        
        
    }

    
    int targetRingBufferId = -1;
    
    public int pump2() {
        // start new script or detect that the end of the data has been reached
        if (targetRingBufferId < 0) {
            // checking EOF first before checking for blocked queue
            if (PrimitiveReader.isEOF(reader)) { 
                return -1;
            }
            pump2startTemplate();
        }        
        return pump2decode();
    }

    private int pump2decode() {
        int result = targetRingBufferId;
        // returns true for end of sequence or group
        if (!decoder.decode(reader)) {  
            // reached the end of the script so close and prep for the next one
            targetRingBufferId = -1;
            PrimitiveReader.closePMap(reader);            
        }
        return result;
    }

    private void pump2startTemplate() {
        // get next token id then immediately start processing the script
        // /read prefix bytes if any (only used by some implementations)
        assert (decoder.preambleDataLength != 0 && decoder.gatherReadData(reader, "Preamble", 0));
        //ring buffer is build on int32s so the implementation limits preamble to units of 4
        assert ((decoder.preambleDataLength&0x3)==0) : "Preable may only be in units of 4 bytes";
        assert (decoder.preambleDataLength<=8) : "Preable may only be 8 or fewer bytes";
        //Hold the preamble value here until we know the template and therefore the needed ring buffer.
        int p = decoder.preambleDataLength;
        int a=0, b=0;
        if (p>0) {
            a = PrimitiveReader.readRawInt(reader);
             if (p>4) {
                b = PrimitiveReader.readRawInt(reader);
                assert(p==8) : "Unsupported large preamble";
            }
        }
        
        // /////////////////
        // open message (special type of group)
        int templateId = PrimitiveReader.openMessage(decoder.maxTemplatePMapSize, reader);
        targetRingBufferId = decoder.activeScriptCursor;
                    
        // write template id at the beginning of this message
        int neededSpace = 1 + decoder.preambleDataLength + decoder.requiredBufferSpace2(templateId, a, b);
        //we know the templateId so we now know which ring buffer to use.
        FASTRingBuffer rb = decoder.ringBuffers[decoder.activeScriptCursor];
        
        if (neededSpace > 0) {
            int size = rb.maxSize;
            if (( size-(rb.addPos.value-rb.remPos.value)) < neededSpace) {
                while (( size-(rb.addPos.value-rb.remPos.value)) < neededSpace) {
                    //TODO: must call blocking policy on this, already committed to read.
                  //  System.err.println("no room in ring buffer");
                   Thread.yield();// rb.dump(rb);
                }
                
            }
        }                   
        
        p = decoder.preambleDataLength;
        if (p>0) {
            //TODO: X, add mode for reading the preamble above but NOT writing to ring buffer because it is not needed.
            FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.addPos, a);
            if (p>4) {
                FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.addPos, b);
            }
        }
        FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.addPos, templateId);
    }
    
    

    
    
    // TODO: B, Check support for group that may be optional
    
    //TODO: A, All data must be communicated throught the ring buffer to support threading however stream state is returned here.
    //return states -1, 0 , 1 for NoDataToRead, Success, NoRoomToWrite
    public int select() {
        // start new script or detect that the end of the data has been reached
        if (decoder.neededSpaceOrTemplate < 0) {
            return beginNewTemplate(decoder, reader, listener);
        }
        return decode(decoder, reader, listener);
    }


    private static int beginNewTemplate(FASTDecoder decoder, PrimitiveReader reader, FASTListener listener ) {
        // checking EOF first before checking for blocked queue
        if (PrimitiveReader.isEOF(reader)) { //REMOVE
            //System.err.println(messageCount);
            return 0;
        }
        int err = hasMoreNextMessage(decoder, reader, listener);
        if (err!=0) {
            return err;
        }
        return decode(decoder, reader, listener);
    }

    private static int decode(FASTDecoder decoder, PrimitiveReader reader, FASTListener listener) {

        // returns true for end of sequence or group
        if (decoder.decode(reader)) {     
            listener.fragment();
            return 1;// has more to read
        } else {
            listener.fragment();
            // reached the end of the script so close and prep for the next one
            decoder.neededSpaceOrTemplate = -1;
            PrimitiveReader.closePMap(reader);
            
//            if (PrimitiveReader.isEOF(reader)) { //replaced with 30001==messageCount and found this method is NOT expensive
//                //System.err.println(messageCount);
//                return 0;
//            }
            
            
            return 2;// finished reading full message
        }   
        
    }

    private static int hasMoreNextMessage(FASTDecoder readerDispatch, PrimitiveReader reader, FASTListener listener) {

        // get next token id then immediately start processing the script
        // /read prefix bytes if any (only used by some implementations)
        assert (readerDispatch.preambleDataLength != 0 && readerDispatch.gatherReadData(reader, "Preamble", 0));
        //ring buffer is build on int32s so the implementation limits preamble to units of 4
        assert ((readerDispatch.preambleDataLength&0x3)==0) : "Preable may only be in units of 4 bytes";
        assert (readerDispatch.preambleDataLength<=8) : "Preable may only be 8 or fewer bytes";
                        
        int result;
        // must have room to store the new template
        //TODO: AA, Must add PEEK method to PrimtiveReader to see what the template is and know which ringBuffer to theck!!
        FASTRingBuffer rb = readerDispatch.ringBuffer(0);//BIG HACK;
        int req = readerDispatch.preambleDataLength + 1;
        if ( (( rb.maxSize-(rb.addPos.value-rb.remPos.value)) < req)) {
            result = 0x80000000;
        } else {
        
            
            //Hold the preamble value here until we know the template and therefore the needed ring buffer.
            int p = readerDispatch.preambleDataLength;
            int a=0, b=0;
            if (p>0) {
                a = PrimitiveReader.readRawInt(reader);
                 if (p>4) {
                    b = PrimitiveReader.readRawInt(reader);
                    assert(p==8) : "Unsupported large preamble";
                }
            }
    
            // /////////////////
            // open message (special type of group)
            
            int templateId = PrimitiveReader.openMessage(readerDispatch.maxTemplatePMapSize, reader);            
            
            readerDispatch.neededSpaceOrTemplate = 0;//already read templateId do not read again
            
            listener.fragment(templateId, readerDispatch.ringBuffer(readerDispatch.activeScriptCursor));
            
            // write template id at the beginning of this message
            result =  readerDispatch.requiredBufferSpace(templateId, a, b);
        }
        return result;

    }

}
