//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.util.ArrayList;
import java.util.List;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.pronghorn.ring.token.TokenBuilder;


public final class FASTOutputByteArrayEquals implements FASTOutput {

	public final byte[] expectedBuffer;
	public int position;
	private DataTransfer dataTransfer;
		

    List<Long> limits = new ArrayList<Long>();
    List<Integer> cursor = new ArrayList<Integer>();
    int[] tokens;
    
	//For testing: this class will confirm that the bytes written match
	//the expected output.
	public FASTOutputByteArrayEquals(byte[] prePopulatedBuffer, int[] tokens) {
		this.expectedBuffer = prePopulatedBuffer;
		this.tokens = tokens;
	}
	
	public void reset() {
		position = 0;
	}
	

	public int position() {
		return position;
	}

	@Override
	public void init(DataTransfer dataTransfer) {
		this.dataTransfer = dataTransfer;
	}

	
	@Override
	public void flush() {
		
		int size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);
		while (size>0) {
			
		    int i = 0;
		    byte[] sourceBuffer = dataTransfer.writer.buffer;
		    byte[] targetBuffer = expectedBuffer;
		    int sIdx = PrimitiveWriter.nextOffset(dataTransfer.writer);
		    int tIdx = position;
		    while (i<size) {
		        
		        if (sourceBuffer[sIdx+i] != targetBuffer[tIdx+i]) {
		            String exp = bin(targetBuffer[tIdx+i])+"/"+hex(targetBuffer[tIdx+i])+"/"+ascii(targetBuffer[tIdx+i]);
		            String fnd = bin(sourceBuffer[sIdx+i])+"/"+hex(sourceBuffer[sIdx+i])+"/"+ascii(sourceBuffer[sIdx+i]);
		            		            
		            FASTException ex = new FASTException("value mismatch at index "+(tIdx+i)+" expected:"+exp+" found:"+fnd );
		            
		            //print context before the error.
		            int j = 0;
		            while (j<i) {
		                exp = bin(targetBuffer[tIdx+j])+"/"+hex(targetBuffer[tIdx+j])+"/"+ascii(targetBuffer[tIdx+j]);
		                fnd = bin(sourceBuffer[sIdx+j])+"/"+hex(sourceBuffer[sIdx+j])+"/"+ascii(sourceBuffer[sIdx+j]);
		                System.err.println("            "+(tIdx+j)+" expected:"+exp+" found:"+fnd+"  "+tokenDetails(sIdx+j)) ;
                        
		                j++;
		            }
		            
		            ///////////////////////////////////////////////////
		            //Scan to determine if this data is right but just mis-aligned due to pmap length change.
		            //this is common when and error in a field also modifies the pmap but the real problem is in the field.
		            ////////////////////////////////////////////////////
		            int alignmentOffset = findLongestedAlignedRun(size, i, sourceBuffer, targetBuffer, sIdx, tIdx);
		                        
		            
		            //print the error 
		            exp = bin(targetBuffer[tIdx+i])+"/"+hex(targetBuffer[tIdx+i])+"/"+ascii(targetBuffer[tIdx+i]);
                    fnd = bin(sourceBuffer[sIdx+i])+"/"+hex(sourceBuffer[sIdx+i])+"/"+ascii(sourceBuffer[sIdx+i]);
                    System.err.println("mismatch at "+(tIdx+i)+" expected:"+exp+" found:"+fnd+"  "+tokenDetails(sIdx+i)) ;
                    i++;
                    
		            //print the aligned context after the error.
		            while (i<size) {
		                exp = bin(targetBuffer[tIdx+i])+"/"+hex(targetBuffer[tIdx+i])+"/"+ascii(targetBuffer[tIdx+i]);
	                    fnd = bin(sourceBuffer[sIdx+i+alignmentOffset])+"/"+hex(sourceBuffer[sIdx+i+alignmentOffset])+"/"+ascii(sourceBuffer[sIdx+i+alignmentOffset]);
		                System.err.println(
		                                    (targetBuffer[tIdx+i]==sourceBuffer[sIdx+i+alignmentOffset] ?  "            " : "mismatch at ")
		                                +(tIdx+i)+" expected:"+exp
		                                +(alignmentOffset>0?" +":"  ")
		                                +(alignmentOffset)+" found:"+fnd+"  "+tokenDetails(sIdx+i+alignmentOffset)); 
		                i++;
		            }
		            		            
		            throw ex;
		            
		        }		
		        i++;
		    }
		    
			
			
			position+=size;
		
			//helpfull as trace
			//System.err.println("data validated up to position:"+position);
			
			size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);
		}
		
		limits.clear();
		cursor.clear();
		
	}

    private int findLongestedAlignedRun(int size, int i, byte[] sourceBuffer, byte[] targetBuffer, int sIdx, int tIdx) {
        int alignmentOffset;

        int longestCount  = 0;
        int longestOffset = 0;		            
        int offset = 3;//tests these offsets
        while (--offset>=0) {
            //try addition
            int k = i;
            int sum = 0;
            while (++k<(size-offset) && sourceBuffer[sIdx+k+offset] == targetBuffer[tIdx+k] ) {//start in the first position afte the mismatch.
                sum++;
            }
            if (sum>longestCount) {
                longestCount = sum;
                longestOffset = offset;
            }
            //try subtraction
            k = i+offset;
            sum = 0;
            while (++k<size && sourceBuffer[sIdx+k-offset] == targetBuffer[tIdx+k] ) {//start in the first position afte the mismatch.
                sum++;
            }
            if (sum>longestCount) {
                longestCount = sum;
                longestOffset = -offset;
            }
        }
        alignmentOffset = longestOffset;

        return alignmentOffset;
    }

    
    private String tokenDetails(long position) {
        
        
        position += 1;//TODO: C, BIG HACK for NOW, we can conpenssate for this by scanning and finding the best alignment with top high bits!!!
        
        
        
        int j = limits.size()-1;
        while (j>=0 && (limits.get(j).longValue()!=position) || 
                ((j>0 && limits.get(j-1).longValue()==position )) //read back to the first field to have ended at this spot, the others would not have written anything.
                ) {
            j--;
        }
        if (j<0) {
            return "";
        }
        
        return "ending "+cursor.get(j)+" "+TokenBuilder.tokenToString(tokens[cursor.get(j)]);
    }

    private String hex(int x) {
        String t = Integer.toHexString(0xFF & x);
        if (t.length() == 1) {
            return '0' + t;
        } else {
            return t;
        }
    }

    private String bin(int x) {
        String t = Integer.toBinaryString(0xFF & x);
        while (t.length() < 8) {
            t = '0' + t;
        }
        return t.substring(t.length() - 8);
    }
    
    private String ascii(int v) {
        int x = v&0x7F;
        if (x<32 || x>126) {
            return "?";
        } else {
            char c = (char)x;
            return ""+c;
        }
    }
    
    public void recordPosition(long limitIdx, int activeScriptCursor) {
        limits.add(limitIdx);
        cursor.add(activeScriptCursor);
    }

}
