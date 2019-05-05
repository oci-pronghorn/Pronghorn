package com.ociweb.pronghorn.util;

import java.util.Arrays;
import static com.ociweb.pronghorn.util.TrieParser.*;

public class TrieParserCompiler {

	//0 normal toTring visit.. (done)
	//1 collect all the data
	//2 static methods all linked
	//3 generate the source 
	
	
	
    public static String genSource(TrieParser trieParser, long unfoundResult, long noMatchResult) {
    	
        return genSource(new StringBuilder(), trieParser.data, 
        		         trieParser.limit, trieParser.SIZE_OF_RESULT, unfoundResult, noMatchResult).toString();
        
    }

    public static StringBuilder genSource(StringBuilder builder, short[] data, int limit, int sizeOfResult,
    		                              long unfoundResult, long noMatchResult) {
    	
    	//TODO: extends the parse Interface which will return a value.
    	
        int i = 0;
        while (i<limit) {
            switch (data[i]) {
           		case TYPE_SWITCH_BRANCH:
            		i = genSourceSwitch(builder, data, i, unfoundResult, noMatchResult);
            		break;
                case TYPE_SAFE_END:
                    i = genSourceSafe(builder, data, i, sizeOfResult, unfoundResult, noMatchResult);
                    break;
                case TYPE_ALT_BRANCH:
                    i = genSourceAltBranch(builder, data, i, unfoundResult, noMatchResult);
                    break;                    
                case TYPE_BRANCH_VALUE:
                    i = genSourceBranchValue(builder, data, i, unfoundResult, noMatchResult);
                    break;
                case TYPE_VALUE_NUMERIC:
                    i = genSourceNumeric(builder, data, i, unfoundResult, noMatchResult);
                    break;
                case TYPE_VALUE_BYTES:
                    i = genSourceBytes(builder, data, i, unfoundResult, noMatchResult);
                    break;
                case TYPE_RUN:
                    i = genSourceRun(builder, data, i, unfoundResult, noMatchResult);  
                    break;
                case TYPE_END:
                    i = genSourceEnd(builder, data, i, sizeOfResult);
                    break;
                default:
                    int remaining = limit-i;
                    builder.append("ERROR Unrecognized value, remaining "+remaining+"\n");
                    if (remaining<100) {
                        builder.append("Remaining:"+Arrays.toString(Arrays.copyOfRange(data, i, limit))+"\n" );
                    }
                    
                    return builder;
            }            
        }
        return builder;
    }
    
    private static int genSourceSwitch(StringBuilder builder, short[] data, int i, long unfoundResult, long noMatchResult) {
		
    	
		Appendables.appendValue(builder.append(METHOD_PREFIX), i).append("(TrieParserReader reader, short[] inputArray, int inputMask, int inputPos) {//switch \n");    	
		
    //	builder.append("SWITCH_");
    	int type = data[i++];
    	int metaBase = i;
    	int meta = data[metaBase];
    	int base = (meta>>8)&0xFF;
    	int trieLen = meta&0xFF;    	
    	
   //   builder.append(base).append("+").append(trieLen).append("[").append(i).append("], \n"); //meta  shift.count
        i++;
        
        int b = i;
        //jumps
        for(int k=0; k<trieLen; k++) {
        	int dist = i-b;
        	int steps = dist>>1;
            int value = (0xFF)&steps+base;
            
            if (data[i]>=0) {
	            if (value<127 && value>=32) {
	 //           	builder.append("case: '").append((char)value).append("' ");
	            } else {
	 //           	builder.append("case: ").append(value).append("   ");
	            }
	        	 
	            int j = data[i]<<15;
	  //      	builder.append(data[i]).append("[").append(i++).append("], ");
	        	i++;
	        	j |= (data[i]&0x7FFF);
	  //      	builder.append(data[i]).append("[").append(i++).append("], ");//JUMP
	        	i++;
	        	j += (metaBase+(trieLen<<1));
	        	
	        	if (j>=data.length) {
	 //       		builder.append("ERROR: OUT OF RANGE ");
	        	}
	 //       	builder.append(" jumpTo:").append(j).append("\n");
            } else {
            	i+=2;
            }
        }
        builder.append("}\n\n");
        return i;

	}


    private static int genSourceNumeric(StringBuilder builder, short[] data, int i, long unfoundResult, long noMatchResult) {
		Appendables.appendValue(builder.append(METHOD_PREFIX), i).append("(TrieParserReader reader, short[] inputArray, int inputMask, int inputPos) {//numeric\n");    	
		
		
	//	if ((reader.localSourcePos = 
		//   parseNumeric(trie.ESCAPE_BYTE, reader, 
		//                   source, reader.localSourcePos, sourceLength-reader.runLength, sourceMask,
		//                    trie.data[reader.pos++])
		//                 )<0) {	
			
	//	}
		
      //  builder.append("EXTRACT_NUMBER");
       // builder.append(data[i]).append("[").append(i++).append("], ");
        i++;
        
      //  builder.append(data[i]).append("[").append(i++).append("], \n");
        i++;
        builder.append("}\n\n");
        return i;
        
    }
    
    private static int genSourceBytes(StringBuilder builder, short[] data, int i, long unfoundResult, long noMatchResult) {
		Appendables.appendValue(builder.append(METHOD_PREFIX), i).append("(TrieParserReader reader, short[] inputArray, int inputMask, int inputPos) {//bytes\n");    	
		
       // builder.append("EXTRACT_BYTES");
       // builder.append(data[i]).append("[").append(i++).append("], ");
        i++;
        
       // builder.append(data[i]).append("[").append(i++).append("], \n");
        i++;
        builder.append("}\n\n");
        return i;
    }
    



    private static int genSourceRun(StringBuilder builder, short[] data, int i, long unfoundResult, long noMatchResult) {
		Appendables.appendValue(builder.append(METHOD_PREFIX), i).append("(TrieParserReader reader, short[] inputArray, int inputMask, int inputPos) {//run\n");    	
		
    //	builder.append("RUN");
    //    builder.append(data[i]).append("[").append(i++).append("], ");
    	i++;
        int len = data[i];
     //   builder.append(data[i]).append("[").append(i++).append("], ");
        i++;
        while (--len >= 0) {
      //      builder.append(data[i]);
            if ((data[i]>=32) && (data[i]<=126)) {
    //            builder.append("'").append((char)data[i]).append("'"); 
            }
    //        builder.append("[").append(i++).append("], ");
            i++;
        }
        builder.append("}\n\n");
        return i;
    }

    private static int genSourceAltBranch(StringBuilder builder, short[] data, int i, long unfoundResult, long noMatchResult) {
		Appendables.appendValue(builder.append(METHOD_PREFIX), i).append("(TrieParserReader reader, short[] inputArray, int inputMask, int inputPos) {//alt branch \n");    	
		
    	//jump to far position, if no match -1 or -2 ???
		//then do the alt..
		
		
   // 	builder.append("ALT_BRANCH");
   //     builder.append(data[i]).append("[").append(i++).append("], "); //TYPE
        i++;
        int j = data[i]<<15;
    //    builder.append(data[i]).append("[").append(i++).append("], ");
        i++;
        j |= (data[i]&0x7FFF);
    //    builder.append(data[i]).append("[").append(i++).append("], jumpTo:"+(i+j));//JUMP
        i++;
        
        
        builder.append("}\n\n");
        return i;
    }

   
    
    

    
    //////////////////////////////////////////
    //////////////////////////////////////////
	
	private static String METHOD_NAME = "m";
    private static String METHOD_PREFIX = "\nprivate static long "+METHOD_NAME;
	
    private static int genSourceBranchValue(StringBuilder builder, 
											short[] data, int i, long unfoundResult, long noMatchResult) {

		Appendables.appendValue(builder.append(METHOD_PREFIX), i).append("(TrieParserReader reader, short[] inputArray, int inputMask, int inputPos) {//branch\n");    	
		
		int p = i+1;    	
		Appendables.appendValue(
		builder.append("    if (0==(TrieParser.computeJumpMask(")
			.append("((short)inputArray[inputMask&inputPos])")
			.append(", ")
			,data[p])
			.append(")&0xFFFFFF)) {\n");    	
		
		//true case
		Appendables.appendValue(builder.append("        return ").append(METHOD_NAME), 
				p+3
				).append("(inputArray, inputMask, inputPos);\n");
		builder.append("    } else {\n");
		//false case
		Appendables.appendValue(builder.append("        return ").append(METHOD_NAME), 
				p+3+((((int)data[p+1])<<15) | (0x7FFF&data[p+2]))
				).append("(inputArray, inputMask, inputPos);\n");
		builder.append("    }\n");
		
		
		builder.append("}\n\n");
		
		//
		return i+4;
	}
    
	private static int genSourceSafe(StringBuilder builder, short[] data, int i, int sizeOfResult, long unfoundResult, long noMatchResult) {
	
		//TODO: still needs captured field data
		//TODO: still needs all callers to use -2 as the end signal.
		
		assert(unfoundResult != noMatchResult);
		
		int followingIdx = 1+i+sizeOfResult;
    	Appendables.appendValue(builder.append(METHOD_PREFIX), i).append("() {//safe point\n"); 
    	
    	//if it is a noMatch at all then we must return the safe point else we must try again or return success
    	Appendables.appendValue(builder.append("    final long result = ").append(METHOD_NAME),followingIdx).append("();\n");
    	Appendables.appendValue(builder.append("    if (result=="),noMatchResult).append(") {;\n");
    	
    	i++;
    	long endResult = TrieParser.readEndValue(data, i, sizeOfResult);        
    	Appendables.appendValue(builder.append("        return "), endResult).append(";\n");        
    	i+=sizeOfResult;
    	
    	builder.append("    } else {\n");    	
    	builder.append("        return result;\n"); 
    	builder.append("    }\n");   
    	
        builder.append("}\n\n");
        
        return i;
    }
	
    private static int genSourceEnd(StringBuilder builder, short[] data, int i, int sizeOfResult) {
    
    	Appendables.appendValue(builder.append(METHOD_PREFIX), i).append("() {//end\n");    	
        i++;
        long endResult = TrieParser.readEndValue(data, i, sizeOfResult);        
        Appendables.appendValue(builder.append("    return "), endResult).append(";\n");        
        i+=sizeOfResult;
        builder.append("}\n\n");
        return i;
        
    }
    
}
