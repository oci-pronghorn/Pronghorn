package com.ociweb.jfast.catalog.extraction;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.esotericsoftware.kryo.io.FastOutput;
import com.ociweb.jfast.catalog.loader.FieldReferenceOffsetManager;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArrayEquals;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputTotals;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferReader;
import com.ociweb.jfast.stream.FASTRingBufferWriter;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;

public class StreamingVisitor implements ExtractionVisitor {

    public static final int CATALOG_TEMPLATE_ID = 0;
    
    RecordFieldExtractor messageTypes;   
    
    byte[] catBytes;
    TemplateCatalogConfig catalog;
    
    long beforeDotValue;
    int beforeDotValueChars;
    long accumValue;
    int accumValueChars;
    long accumSign;
    boolean aftetDot;
    boolean startingMessage = true;
    long offestForTemplateId = -1;
    
    
    static final long[] POW_10;
    
    static {
    	int max = 19;
    	POW_10 = new long[max];
    	int j = 0;
    	long total=1;
    	while (j<max) {
    		POW_10[j++]=total;
    		total *= 10l;
    	}    	
    	//System.err.println(Arrays.toString(POW_10));
    }
    
    
    //chars are written to  ring buffer.
    
    int bytePosActive;
    int bytePosStartField;
    
    FASTRingBuffer ringBuffer;
 
	FileOutputStream fost;	
	FASTOutputStream fastOutput;
	
    int writeBuffer = 2048;
    PrimitiveWriter writer;
    FASTDynamicWriter dynamicWriter = null;
    
    
    public StreamingVisitor(RecordFieldExtractor messageTypes) {
    	
        
    	File fastFile = new File("/home/nate/flat/fastOut2.fast");
    	
    	
    	try {
			fost = new FileOutputStream(fastFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	fastOutput = new FASTOutputStream(fost);
    	writer = new PrimitiveWriter(writeBuffer, fastOutput, true);
    	
        
    	//initial ring buffer needed only for the first catalog then we move on from this one.
    	byte[] catBytes = messageTypes.getCatBytes();
    	if (null==catBytes) {
    		catBytes = messageTypes.memoizeCatBytes(); //must use a ring buffer from catalog or it will not be initilzied for use.
    	}
    	ringBuffer = (new TemplateCatalogConfig(catBytes)).ringBuffers().buffers[0];
    			 
    	ringBufferList.add(ringBuffer);
    	
        this.messageTypes = messageTypes;    
        
        messageTypes.resetToRecordStart();
        
        aftetDot = false;
        beforeDotValue = 0;
        beforeDotValueChars = 0;
        
        accumSign = 1;
        
        accumValue = 0;
        accumValueChars = 0;
        
    }
        
    //TODO: if there is a failure, store the position and roll back to there and try the next choice
    //      best idea out of those so far.
    
    
    @Override
    public void appendContent(MappedByteBuffer mappedBuffer, int pos, int limit, boolean contentQuoted) {
                
    	
    	
        //discovering the field types using the same way the previous visitor did it
        messageTypes.appendContent(mappedBuffer, pos, limit, contentQuoted);
                  
        //keep bytes here in case we need it, will only be known after we are done
        int p = pos;
        while (p<limit) {
            byte b = mappedBuffer.get(p);
            
            //TODO: confirm if this is numeric that non of this is kept!
            ringBuffer.byteBuffer[ringBuffer.byteMask&bytePosActive++] = b; //TODO: need to check for the right stop point
                        
            if ('.' == b) {
                aftetDot = true;
                beforeDotValue = accumValue;
                beforeDotValueChars = accumValueChars;
                accumValue = 0;
                accumValueChars = 0;
            } else {
               if ('+' != b) {
                   if ('-' == b) { 
                       accumSign = -1;
                   } else {
                       int v = (b-'0');
                       accumValue = (10*accumValue) + v;    
                       accumValueChars++;
                   }                    
               }
            }
            p++;
        }                 
        
    }

    String lastClosedLine = "";
    
    @Override
    public void closeRecord(int startPos) {
        
    	
    	lastClosedLine = new String(messageTypes.bytesForLine(startPos));
    	
    //	System.err.println(offestForTemplateId+" "+messageTypes.templateId()+" old pos "+ringBuffer.headPos+" new pos "+ringBuffer.workingHeadPos.value);
    	
    	//write tid to point in stream before we publish.
    	FASTRingBuffer.setValue(ringBuffer.buffer, ringBuffer.mask, offestForTemplateId, messageTypes.templateId());
    	offestForTemplateId = -1;
    	
    	
        messageTypes.resetToRecordStart();
        
        
        //move the pointer up to the next record
        bytePosStartField = ringBuffer.addByteWorkingHeadPos.value = bytePosActive;
        
        FASTRingBuffer.publishWrites(ringBuffer);
        startingMessage = true;       
        
        messageTypes.totalRecords++;
               
    //    FASTRingBuffer.dump(ringBuffer);
        
    	if (null!=dynamicWriter) {
    		//can move next is NOT moving the working tail to tail until the NEXT call of can move
	        while (FASTRingBuffer.canMoveNext(ringBuffer)) {

	            try{   
	                dynamicWriter.write();
	            } catch (FASTException e) {
	               // System.err.println("ERROR: cursor at "+writerDispatch.getActiveScriptCursor()+" "+TokenBuilder.tokenToString(queue.from.tokens[writerDispatch.getActiveScriptCursor()]));
	                throw e;
	            }                            

	        }
	        
	        //the working tail is up to head but its not copied over to tail yet why not?
	        assert(0==FASTRingBuffer.contentRemaining(ringBuffer));
	        //NOT sure why this reset is needed. TODO: nothing should be left behind after there is no content.
	        ringBuffer.reset();
	        
    	}
        
    }

    @Override
    public boolean closeField(int startPos) {
    	
    	if (startingMessage) {
    		 offestForTemplateId = ringBuffer.workingHeadPos.value++; 
    	}    	
    	
    	//TODO: if we have multiple choices we will need branch prediction to try the most likely
    	//      upon failure mark it and return false to read from the start of the message again.
    	
        //selecting the message type one field at at time as we move forward
        int fieldType = messageTypes.convertRawTypeToSpecific(messageTypes.moveNextField());
        
        switch (fieldType) {
            case RecordFieldExtractor.TYPE_NULL:
            	
            	System.err.println("last closed line:\n"+lastClosedLine);
            	String example = new String(messageTypes.bytesForLine(startPos));
            	
                //TODO: what optional types are available? what if there are two then follow the order.
                new Exception("Require optional field but unable to find one in field "+messageTypes.fieldCount+" recs "+messageTypes.totalRecords+" example "+example).printStackTrace();;
                System.exit(-1);
                
                break;
            case RecordFieldExtractor.TYPE_UINT:                
            case RecordFieldExtractor.TYPE_SINT:
//            	if (accumValueChars==0) {
//            		System.err.println("writing int null");
//            	}
            	
                FASTRingBufferWriter.writeInt(ringBuffer, (int)(accumValue*accumSign));  
                break;   
            case RecordFieldExtractor.TYPE_ULONG:
            case RecordFieldExtractor.TYPE_SLONG:
//            	if (accumValueChars==0) {
//            		System.err.println("writing long null");
//            	}
                FASTRingBufferWriter.writeLong(ringBuffer, accumValue*accumSign);  
                break;    
            case RecordFieldExtractor.TYPE_ASCII:
            case RecordFieldExtractor.TYPE_BYTES:  
//            	if ((bytePosActive-bytePosStartField)==0) {
//            		//System.err.println("writing text null "+(bytePosActive-bytePosStartField));
//            		FASTRingBufferWriter.finishWriteBytes(ringBuffer, -1);
//            	} else {
            	
            	//System.err.println("len"+(bytePosActive-bytePosStartField)+" vs "+new String(messageTypes.bytesForLine(startPos)));
            	
            		FASTRingBufferWriter.finishWriteBytes(ringBuffer, bytePosActive-bytePosStartField);
            //	}
                break;
            case RecordFieldExtractor.TYPE_DECIMAL:
            	
            	//TODO: need to know about constant if that is the case
            	//TODO: using a constant global exponent is a very bad idea for compression size.
                int exponent = accumValueChars;//messageTypes.globalExponent(); //always positive, positions measured after the dot
                             
                
                if (accumValueChars>=POW_10.length) {
                	//this caused by bug totaling the chars that no longer happens. but this may be a good idea for long odd values
                	//they should be ascii instead???
                	System.err.println("Too long example:"+new String(messageTypes.bytesForLine(startPos)));
                }
                
                //this is A solution but it is not THE global solution
                long totalValue = (beforeDotValue*POW_10[accumValueChars])+accumValue;
                int  totalExp = accumValueChars;
                
                //if totalExp is larger than const exponent must /10 to total value and dec by that many
                while (totalExp>exponent) {
                	totalValue=totalValue/10;
                	totalExp--;
                }
                
                //if totalExp is smaller than const exponent must *10 to total value and inc by that many
                while (totalExp<exponent) {
                	totalValue=totalValue*10;
                	totalExp++;
                }
                
//            	if (accumValueChars==0) {
//            		System.err.println("writing decimal null");
//            	}
                
                FASTRingBufferWriter.writeDecimal(ringBuffer, exponent, totalValue*accumSign);  
                break;
            
            default:
                throw new UnsupportedOperationException("Field was "+fieldType);
        }       
        
        //closing field so keep this new active position as the potential start for the next field
        bytePosStartField = bytePosActive;
        // ** write as we go close out the field

        resetFieldStateCounters();
                
        startingMessage = false;
        
        return true;//this was successful so continue
        
    }


	private void resetFieldStateCounters() {
		aftetDot = false;
        beforeDotValue = 0;
        accumSign = 1;
        accumValue = 0;
        beforeDotValueChars = 0;
        accumValueChars = 0;
	}


    
    @Override
    public void closeFrame() {   
    	System.err.println("****************** close frame:");
    	boolean debug = true;
    	if (debug) {
    		System.err.println(messageTypes.totalRecords+"\n"+lastClosedLine);
    	}
    	   
    	System.err.println("ringBuffer Ints:"+ringBuffer.workingHeadPos.value+" written FAST:"+PrimitiveWriter.totalWritten(writer));
    	
    }

    //TODO: hack test for now this should be a ring buffer of ring buffers.
    List<FASTRingBuffer> ringBufferList = new ArrayList<FASTRingBuffer>();

    @Override
    public void openFrame() {
    	System.err.println("****************** open frame:");
    	
    	resetFieldStateCounters();
    	messageTypes.resetToRecordStart();
    	//if any partial write of field data is in progress just throw it away because 
    	//next frame will begin again from the start of the message.
    	bytePosStartField = bytePosActive = ringBuffer.addBytesHeadPos.intValue();
    	FASTRingBuffer.abandonWrites(ringBuffer);
    	    	
        //get new catalog if is has been changed by the other visitor
        byte[] catBytes = messageTypes.getCatBytes();
        if (!Arrays.equals(this.catBytes, catBytes)) {
            this.catBytes = catBytes;        
            catalog = new TemplateCatalogConfig(catBytes);
            System.err.println("new catalog");            
            
            //TODO: A, need to build FieldRectordExtractor so it matches the existing catalog, then we can start with a given catalog and remove this dynamic part.
            
            //TODO: check assumption that templateID 0 is the one for sending catalogs.
            
            
            // Write new catalog to old stream stream so it is the last one written.
            FASTRingBufferWriter.writeInt(ringBuffer, CATALOG_TEMPLATE_ID);        
            FASTRingBufferWriter.writeBytes(ringBuffer, catBytes);               
            
			//System.err.println("length "+catBytes.length);
			//System.err.println(Arrays.toString(catBytes));
            
            FASTRingBuffer.publishWrites(ringBuffer);
         //   System.err.println("C "+ringBuffer.contentRemaining(ringBuffer)+"  "+catBytes.length);
            
            //now create new ring buffer and chain them
            FASTRingBuffer newRingBuffer = catalog.ringBuffers().buffers[0];
            
            ringBufferList.add(newRingBuffer);
            
            //TODO: chain these ring buffers
             ringBuffer = newRingBuffer;
             

             //TODO: this fine for the static template but not sure about the dynamic one.
             FASTEncoder writerDispatch = 
            		// new FASTWriterInterpreterDispatch(catalog);
            		 DispatchLoader.loadDispatchWriter(catBytes); 
             dynamicWriter = new FASTDynamicWriter(writer, ringBuffer, writerDispatch);

        }        
    }


	public FASTRingBuffer getRingBuffer(int index) {
		//TODO: clear all previous values.
		//TODO: must block until first rb is found?
		while (ringBufferList.size()<=0) {
			Thread.yield();//TODO: poor choice but still thinking about this.
		}
		return ringBufferList.get(index);
	//	return ringBuffer;
	}

	public void printResults() {
		
		fastOutput.flush();
		
		// TODO Auto-generated method stub
		
	}



}
