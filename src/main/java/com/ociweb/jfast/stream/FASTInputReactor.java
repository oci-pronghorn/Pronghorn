//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
                // TODO Auto-generated method stub
                
            }

            @Override
            public void fragment() {
                // TODO Auto-generated method stub
                
            }};
        
    }

    Runnable run = new Runnable() {

        @Override
        public void run() {
            int i = 0;
            while (pump()) {
                if (0==(0xFFF&i++)) {
                    Thread.yield();//Do no yield between every fragment, it slows down way too much. 
                }
                
            }
            
    }};
    
    public FASTInputReactor(FASTDecoder decoder, PrimitiveReader reader, FASTListener listener) {
        this.decoder=decoder;
        this.reader=reader;
        this.listener = listener;
    }
    
    public void start(ExecutorService executorService) {
        executorService.execute(run);        
    }
    
    public Runnable runnable() {
        return run;
    }
    
    public boolean pump() {

        if (decoder.decode(reader)) {     
            listener.fragment();
            return true;// has more to read
        } else {
            PrimitiveReader.closePMap(reader);            
            if (PrimitiveReader.isEOF(reader)) { 
                listener.fragment();
                //System.err.println(messageCount);
                return false;
            }
            hasMoreNextMessage(decoder, reader, listener);
            listener.fragment();
            return true;// finished reading full message
        }   
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
                        
        
        // must have room to store the new template
        //TODO: AA, Must add PEEK method to PrimtiveReader to see what the template is and know which ringBuffer to theck!!
        FASTRingBuffer rb = readerDispatch.ringBuffer(0);//BIG HACK;
        int req = readerDispatch.preambleDataLength + 1;
        if ( (( rb.maxSize-(rb.addPos.value-rb.remPos.value)) < req)) {
            return 0x80000000;
        }
        
        
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
        return readerDispatch.requiredBufferSpace(templateId, a, b);
        

    }

}
