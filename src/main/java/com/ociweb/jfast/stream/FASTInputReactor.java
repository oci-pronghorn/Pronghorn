//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

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
public class FASTInputReactor {

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

    // TODO: B, Check support for group that may be optional
    public static int select(FASTDecoder decoder, PrimitiveReader reader) {
        // start new script or detect that the end of the data has been reached
        if (decoder.neededSpaceOrTemplate < 0) {
            return beginNewTemplate(decoder, reader, decoder.ringBuffer());
        }
        return checkSpaceAndDecode(decoder, reader, decoder.ringBuffer());
    }


    private static int beginNewTemplate(FASTDecoder decoder, PrimitiveReader reader, FASTRingBuffer rb) {
        // checking EOF first before checking for blocked queue
        if (PrimitiveReader.isEOF(reader)) { //replaced with 30001==messageCount and found this method is NOT expensive
            //System.err.println(messageCount);
            return 0;
        }
        // must have room to store the new template
        int req = decoder.preambleDataLength + 1;
        if ( (( rb.maxSize-(rb.addPos-rb.remPos)) < req)) {
            return 0x80000000;
        }
        decoder.neededSpaceOrTemplate=hasMoreNextMessage(req, decoder, reader, rb);
        return checkSpaceAndDecode(decoder, reader, rb);
    }

    private static int checkSpaceAndDecode(FASTDecoder decoder, PrimitiveReader reader, FASTRingBuffer rb) {
        if (decoder.neededSpaceOrTemplate > 0) {
            if (( rb.maxSize-(rb.addPos-rb.remPos)) < decoder.neededSpaceOrTemplate) {
                return 0x80000000;
            }
            decoder.neededSpaceOrTemplate = 0;
        }        
        // returns true for end of sequence or group
        return decoder.decode(reader) ? sequence(decoder, rb, reader) : finishTemplate(rb, reader, decoder);
    }

    private static final int sequence(FASTDecoder decoder, FASTRingBuffer rb, PrimitiveReader reader) {
        rb.unBlockSequence();//TODO: expensive call change to static?
        if ( decoder.readyToDoSequence) { // jumping (backward) to do this sequence again.
            decoder.readyToDoSequence = false;
            return 1;// has group to read
        } else {
            // finished sequence, no need to jump
            if (1+decoder.activeScriptCursor == decoder.activeScriptLimit) {
                decoder.neededSpaceOrTemplate = -1;
                PrimitiveReader.closePMap(reader);
                return 3;// finished reading full message and the sequence
            }
            return 1;// has group to read
        }
    }

    private static int hasMoreNextMessage(int req, FASTDecoder readerDispatch, PrimitiveReader reader, FASTRingBuffer rb) {

        // get next token id then immediately start processing the script
        // /read prefix bytes if any (only used by some implementations)
        assert (readerDispatch.preambleDataLength != 0 && readerDispatch.gatherReadData(reader, "Preamble"));
        //ring buffer is build on int32s so the implementation limits preamble to units of 4
        assert ((readerDispatch.preambleDataLength&0x3)==0) : "Preable may only be in units of 4 bytes";
        assert (readerDispatch.preambleDataLength<=8) : "Preable may only be 8 or fewer bytes";
                
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
        
        //we know the templateId so we now know which ring buffer to use.
        //TODO: X, add mode for reading the preamble above but NOT writing to ring buffer because it is not needed.
        p = readerDispatch.preambleDataLength;
        if (p>0) {
            rb.buffer[rb.mask & rb.addPos++] = a;
            if (p>4) {
                rb.buffer[rb.mask & rb.addPos++] = b;
            }
        }
        rb.buffer[rb.mask & rb.addPos++] = templateId;
                
        // write template id at the beginning of this message
        return readerDispatch.requiredBufferSpace(templateId);
        

    }

    private static final int finishTemplate(FASTRingBuffer ringBuffer, PrimitiveReader reader, FASTDecoder decoder) {
        // reached the end of the script so close and prep for the next one
        ringBuffer.unBlockMessage();
        decoder.neededSpaceOrTemplate = -1;
        PrimitiveReader.closePMap(reader);
        return 2;// finished reading full message
    }

}
