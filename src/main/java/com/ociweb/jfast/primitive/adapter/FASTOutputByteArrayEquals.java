//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.util.ArrayList;
import java.util.List;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;


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

	
	/*
4 Write: Group:010000/Open:DynTempl::000010/17
6 Write: ASCII:001000/Constant:000010/10
6 Write: ASCII:001000/Constant:000010/11
6 Write: ASCII:001000/Constant:000010/12
6 Write: IntegerUnsigned:000000/None:000000/17
7 Write: IntegerUnsigned:000000/None:000000/18
10 Write: ASCIIOptional:001001/None:000000/13
11 Write: Length:010100/None:000000/19
12 Write: Group:010000/Open:Seq:PMap::001100/8
15 Write: ASCII:001000/Constant:000010/14
15 Write: LongUnsignedOptional:000101/None:000000/2
16 Write: IntegerUnsignedOptional:000001/Default:000011/20
16 Write: LongUnsigned:000100/None:000000/3
19 Write: IntegerUnsigned:000000/Default:000011/21
19 Write: IntegerUnsigned:000000/None:000000/22
20 Write: IntegerUnsigned:000000/Constant:000010/23
20 Write: Group:010000/Close:Seq:PMap::001101/8
22 Write: Group:010000/Open:DynTempl::000010/32
24 Write: Dictionary:011000/Reset:000000/1
24 Write: ASCII:001000/Constant:000010/1
24 Write: ASCII:001000/Constant:000010/2
24 Write: ASCII:001000/Constant:000010/3
24 Write: IntegerUnsigned:000000/None:000000/0
25 Write: IntegerUnsigned:000000/None:000000/1
28 Write: IntegerUnsigned:000000/None:000000/2
32 Write: Length:010100/None:000000/3
33 Write: Group:010000/Open:Seq:PMap::001100/22
36 Write: IntegerUnsigned:000000/Copy:000001/4
36 Write: IntegerUnsignedOptional:000001/Default:000011/5
37 Write: ASCII:001000/Copy:000001/4
38 Write: IntegerUnsignedOptional:000001/None:000000/6
39 Write: IntegerUnsigned:000000/Constant:000010/7
39 Write: IntegerUnsigned:000000/Copy:000001/8
40 Write: IntegerUnsigned:000000/Increment:000101/9
41 Write: Decimal:001100/Default:000011/10
42 Write: IntegerUnsigned:000000/Copy:000001/11
45 Write: IntegerSignedOptional:000011/Delta:000100/12
46 Write: IntegerUnsignedOptional:000001/Delta:000100/13
47 Write: ASCIIOptional:001001/Default:000011/5
47 Write: DecimalOptional:001101/Default:000011/14
49 Write: IntegerUnsignedOptional:000001/Default:000011/15
50 Write: ASCIIOptional:001001/Default:000011/6
51 Write: ASCIIOptional:001001/Default:000011/7
52 Write: ASCIIOptional:001001/Default:000011/8
53 Write: IntegerUnsignedOptional:000001/Default:000011/16
54 Write: ASCIIOptional:001001/Default:000011/9
54 Write: Group:010000/Close:Seq:PMap::001101/22
53 Write: Group:010000/Open:Seq:PMap::001100/22
56 Write: IntegerUnsigned:000000/Copy:000001/4
56 Write: IntegerUnsignedOptional:000001/Default:000011/5
56 Write: ASCII:001000/Copy:000001/4
56 Write: IntegerUnsignedOptional:000001/None:000000/6
57 Write: IntegerUnsigned:000000/Constant:000010/7
57 Write: IntegerUnsigned:000000/Copy:000001/8
57 Write: IntegerUnsigned:000000/Increment:000101/9
57 Write: Decimal:001100/Default:000011/10
58 Write: IntegerUnsigned:000000/Copy:000001/11
61 Write: IntegerSignedOptional:000011/Delta:000100/12
62 Write: IntegerUnsignedOptional:000001/Delta:000100/13
63 Write: ASCIIOptional:001001/Default:000011/5
63 Write: DecimalOptional:001101/Default:000011/14
65 Write: IntegerUnsignedOptional:000001/Default:000011/15
66 Write: ASCIIOptional:001001/Default:000011/6
67 Write: ASCIIOptional:001001/Default:000011/7
68 Write: ASCIIOptional:001001/Default:000011/8
69 Write: IntegerUnsignedOptional:000001/Default:000011/16
70 Write: ASCIIOptional:001001/Default:000011/9
70 Write: Group:010000/Close:Seq:PMap::001101/22
73 Write: Group:010000/Open:DynTempl::000010/32
75 Write: Dictionary:011000/Reset:000000/1
75 Write: ASCII:001000/Constant:000010/1
75 Write: ASCII:001000/Constant:000010/2
75 Write: ASCII:001000/Constant:000010/3
75 Write: IntegerUnsigned:000000/None:000000/0
76 Write: IntegerUnsigned:000000/None:000000/1
79 Write: IntegerUnsigned:000000/None:000000/2
83 Write: Length:010100/None:000000/3
84 Write: Group:010000/Open:Seq:PMap::001100/22
87 Write: IntegerUnsigned:000000/Copy:000001/4
88 Write: IntegerUnsignedOptional:000001/Default:000011/5
89 Write: ASCII:001000/Copy:000001/4
90 Write: IntegerUnsignedOptional:000001/None:000000/6
91 Write: IntegerUnsigned:000000/Constant:000010/7
91 Write: IntegerUnsigned:000000/Copy:000001/8
92 Write: IntegerUnsigned:000000/Increment:000101/9
93 Write: Decimal:001100/Default:000011/10
94 Write: IntegerUnsigned:000000/Copy:000001/11
97 Write: IntegerSignedOptional:000011/Delta:000100/12
98 Write: IntegerUnsignedOptional:000001/Delta:000100/13
99 Write: ASCIIOptional:001001/Default:000011/5
99 Write: DecimalOptional:001101/Default:000011/14
101 Write: IntegerUnsignedOptional:000001/Default:000011/15
102 Write: ASCIIOptional:001001/Default:000011/6
103 Write: ASCIIOptional:001001/Default:000011/7
104 Write: ASCIIOptional:001001/Default:000011/8
105 Write: IntegerUnsignedOptional:000001/Default:000011/16
106 Write: ASCIIOptional:001001/Default:000011/9
106 Write: Group:010000/Close:Seq:PMap::001101/22
105 Write: Group:010000/Open:Seq:PMap::001100/22
108 Write: IntegerUnsigned:000000/Copy:000001/4
108 Write: IntegerUnsignedOptional:000001/Default:000011/5
108 Write: ASCII:001000/Copy:000001/4
108 Write: IntegerUnsignedOptional:000001/None:000000/6
109 Write: IntegerUnsigned:000000/Constant:000010/7
109 Write: IntegerUnsigned:000000/Copy:000001/8
109 Write: IntegerUnsigned:000000/Increment:000101/9
109 Write: Decimal:001100/Default:000011/10
110 Write: IntegerUnsigned:000000/Copy:000001/11
113 Write: IntegerSignedOptional:000011/Delta:000100/12
114 Write: IntegerUnsignedOptional:000001/Delta:000100/13
115 Write: ASCIIOptional:001001/Default:000011/5
115 Write: DecimalOptional:001101/Default:000011/14
117 Write: IntegerUnsignedOptional:000001/Default:000011/15
118 Write: ASCIIOptional:001001/Default:000011/6
119 Write: ASCIIOptional:001001/Default:000011/7
120 Write: ASCIIOptional:001001/Default:000011/8
121 Write: IntegerUnsignedOptional:000001/Default:000011/16
122 Write: ASCIIOptional:001001/Default:000011/9
122 Write: Group:010000/Close:Seq:PMap::001101/22
121 Write: Group:010000/Open:Seq:PMap::001100/22
124 Write: IntegerUnsigned:000000/Copy:000001/4
124 Write: IntegerUnsignedOptional:000001/Default:000011/5
125 Write: ASCII:001000/Copy:000001/4
125 Write: IntegerUnsignedOptional:000001/None:000000/6
126 Write: IntegerUnsigned:000000/Constant:000010/7
126 Write: IntegerUnsigned:000000/Copy:000001/8
126 Write: IntegerUnsigned:000000/Increment:000101/9
126 Write: Decimal:001100/Default:000011/10
127 Write: IntegerUnsigned:000000/Copy:000001/11
130 Write: IntegerSignedOptional:000011/Delta:000100/12
131 Write: IntegerUnsignedOptional:000001/Delta:000100/13
132 Write: ASCIIOptional:001001/Default:000011/5
132 Write: DecimalOptional:001101/Default:000011/14
134 Write: IntegerUnsignedOptional:000001/Default:000011/15
135 Write: ASCIIOptional:001001/Default:000011/6
136 Write: ASCIIOptional:001001/Default:000011/7
137 Write: ASCIIOptional:001001/Default:000011/8
138 Write: IntegerUnsignedOptional:000001/Default:000011/16
139 Write: ASCIIOptional:001001/Default:000011/9
139 Write: Group:010000/Close:Seq:PMap::001101/22
	 */
	
	
	@Override
	public void flush() {
		
		int size = dataTransfer.nextBlockSize();
		while (size>0) {
			
		    int i = 0;
		    byte[] sourceBuffer = dataTransfer.rawBuffer();
		    byte[] targetBuffer = expectedBuffer;
		    int sIdx = dataTransfer.nextOffset();
		    int tIdx = position;
		    while (i<size) {
		        
		        //TODO: convert from internal buffer position into real file position.
		        
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
			
			size = dataTransfer.nextBlockSize();
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
        
        
        position+=1;//TODO: C, BIG HACK for NOW, we can conpenssate for this by scanning and finding the best alignment with top high bits!!!
        
        
        
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
