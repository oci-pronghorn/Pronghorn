package com.ociweb.pronghorn.network.http;

import static com.ociweb.pronghorn.pipe.Pipe.blobMask;
import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;

import java.io.IOException;
import java.util.Arrays;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.AppendableBuilder;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.math.Decimal;
import com.ociweb.pronghorn.util.math.DecimalResult;
import com.ociweb.pronghorn.util.math.RationalResult;

public class FieldExtractionDefinitions {

	private final TrieParser runtimeParser;
	private int indexCount;
	public final int groupId;
	public final int pathId;
	public int defaultsCount = 0;
	private transient Pipe<RawDataSchema> workingPipe = null;

	public static final int DEFAULT_VALUE_FLAG       = 1<<30;
	public static final int DEFAULT_VALUE_FLAG_MASK  = DEFAULT_VALUE_FLAG-1;
		
	private static final TrieParser numberParser = textToNumberTrieParser();
		
	public FieldExtractionDefinitions(boolean trustText, int groupId, int pathId) {
		this.runtimeParser = new TrieParser(64, 2, trustText, true);
		this.groupId = groupId;
		this.pathId = pathId;

	}

	
	private static TrieParser textToNumberTrieParser() {
		 TrieParser p = new TrieParser(8,true); //supports only complete values
		 p.setUTF8Value("%i%.%/%.", 1); 
		 return p;
	}
	
	public TrieParser getRuntimeParser() {
		return runtimeParser;
	}
	
	public void setIndexCount(int indexCount) {
		this.indexCount = indexCount;
	}
	
	public int getIndexCount() {
		return this.indexCount;
	}


	private long tempNumM;
	private byte tempNumE;
	
	private long tempDenM;
	private byte tempDenE;
	
	private long tempNumerator;
	private long tempDenominator;
			
	private long tempDecimalM;
	private byte tempDecimalE;
	
	DecimalResult numerator = new DecimalResult() {
		@Override
		public void result(long m, byte e) {
			tempNumM = m;
			tempNumE = e;
		}
	};
	
	DecimalResult denominator = new DecimalResult() {
		@Override
		public void result(long m, byte e) {
			tempDenM = m;
			tempDenE = e;
		}
	};
	
	RationalResult rational = new RationalResult() {
		@Override
		public void result(long numerator, long denominator) {
			tempNumerator = numerator;
			tempDenominator = denominator;
		}
	};
	
	DecimalResult decimal = new DecimalResult() {
		@Override
		public void result(long m, byte e) {
			tempDecimalM = m;
			tempDecimalE = e;
		}
	};
	
	private void setBytes(int valueIndex) {
		
		CharSequence cs = defaultText[valueIndex];
		
		
	  	if (null==workingPipe || (cs.length()*6) > workingPipe.maxVarLen) {
    		workingPipe = RawDataSchema.instance.newPipe(2,cs.length()*6);
    		workingPipe.initBuffers();
    	}
	  	
	  	defaultBytes[valueIndex] = convertUTF8toBytes(cs, workingPipe);
	}

	private byte[] convertUTF8toBytes(CharSequence cs, Pipe<RawDataSchema> localPipe) {
		Pipe.addMsgIdx(localPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        
        int origPos = Pipe.getWorkingBlobHeadPosition(localPipe);
        Pipe.addBytePosAndLen(localPipe, origPos, Pipe.copyUTF8ToByte(cs, 0, cs.length(), localPipe));        
        Pipe.publishWrites(localPipe);
        Pipe.confirmLowLevelWrite(localPipe, Pipe.sizeOf(localPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        ///
        
        Pipe.takeMsgIdx(localPipe);
        
		int mta = Pipe.takeRingByteMetaData(localPipe);
		int len = Pipe.takeRingByteLen(localPipe);
		int mask = blobMask(localPipe);	
    	int pos = bytePosition(mta, localPipe, len)&mask;     		
		byte[] backing = byteBackingArray(mta, localPipe);
		
		byte[] result = new byte[len];
		Pipe.copyBytesFromToRing(backing, pos, mask, 
				                 result, 0, Integer.MAX_VALUE, 
				                 len);
		
        Pipe.confirmLowLevelRead(localPipe, Pipe.sizeOf(localPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        Pipe.releaseReadLock(localPipe);
		return result;
	}
	
	public void defaultText(TrieParserReader reader, String key, String value) {
		//if key is not found continue
		if (-1 == reader.query(runtimeParser, key)) {
			//get next index
			int defaultValueIndex = defaultsCount++;
			//grow if needed
			growDefaults(defaultsCount);
			
			defaultText[defaultValueIndex] = value;
			setBytes(defaultValueIndex);
			
			long id = TrieParserReader.query(reader, numberParser, value);
						
			if (id>=0) {
			
				long aM = TrieParserReader.capturedDecimalMField(reader, 0); 
				byte aE = TrieParserReader.capturedDecimalEField(reader, 0);
				
				long bM = TrieParserReader.capturedDecimalMField(reader, 1); 
				byte bE = TrieParserReader.capturedDecimalEField(reader, 1);		
				
				long cM = TrieParserReader.capturedDecimalMField(reader, 2); 
				byte cE = TrieParserReader.capturedDecimalEField(reader, 2);	
				
				long dM = TrieParserReader.capturedDecimalMField(reader, 3); 
				byte dE = TrieParserReader.capturedDecimalEField(reader, 3);
							
				Decimal.sum(aM, aE, bM, bE, numerator);
				Decimal.sum(cM, cE, dM, dE, denominator);
				
				Decimal.asRational(tempNumM, tempNumE, 
						           tempDenM, tempDenE, 
						           rational);
				
				defaultNumerator[defaultValueIndex] = tempNumerator;
				defaultDenominator[defaultValueIndex] = tempDenominator;
				defaultDouble[defaultValueIndex] = (double)tempNumerator/(double)tempDenominator;
				
				Decimal.fromRational(tempNumerator, tempDenominator, decimal);
				
				defaultDecimalM[defaultValueIndex] = tempDecimalM;
				defaultDecimalE[defaultValueIndex] = tempDecimalE;
						
				defaultIntegers[defaultValueIndex] = Decimal.asLong(tempDecimalM, tempDecimalE);
		
			} else {
				//not a recognized number so do not set			
			}
			
								
			//add to map key bytes and the index with mask.
			runtimeParser.setUTF8Value(key, DEFAULT_VALUE_FLAG|defaultValueIndex);
		}
	}

	public void defaultInteger(TrieParserReader reader, String key, long value) {
		//if key is not found continue
		if (-1 == reader.query(runtimeParser, key)) {
			//get next index
			int defaultValueIndex = defaultsCount++;
			//grow if needed
			growDefaults(defaultsCount);
			
			defaultIntegers[defaultValueIndex] = value;
			
			defaultText[defaultValueIndex] = Appendables.appendValue(new StringBuilder(), value);
			setBytes(defaultValueIndex);
			
			defaultNumerator[defaultValueIndex] = value;
			defaultDenominator[defaultValueIndex] = 1;
			
			defaultDecimalM[defaultValueIndex] = value;
			defaultDecimalE[defaultValueIndex] = 0;
			
			defaultDouble[defaultValueIndex] = value;
			
			//add to map key bytes and the index with mask.
			runtimeParser.setUTF8Value(key, DEFAULT_VALUE_FLAG|defaultValueIndex);
		}
	}
	
	public void defaultDecimal(TrieParserReader reader, String key, long m, byte e) {
		//if key is not found continue
		if (-1 == reader.query(runtimeParser, key)) {
			//get next index
			int defaultValueIndex = defaultsCount++;
			//grow if needed
			growDefaults(defaultsCount);
			
			defaultIntegers[defaultValueIndex] = Decimal.asLong(m, e);
			
			defaultNumerator[defaultValueIndex] = Decimal.asNumerator(m,e);
			defaultDenominator[defaultValueIndex] = Decimal.asDenominator(e);
			
			defaultText[defaultValueIndex] = Appendables.appendDecimalValue(new StringBuilder(), m, e);
			setBytes(defaultValueIndex);
			
			defaultDecimalM[defaultValueIndex] = m;
			defaultDecimalE[defaultValueIndex] = e;
			
			defaultDouble[defaultValueIndex] = Decimal.asDouble(m, e);
									
			//add to map key bytes and the index with mask.
			runtimeParser.setUTF8Value(key, DEFAULT_VALUE_FLAG|defaultValueIndex);
		}
	}

	private final DecimalResult recordDecimalResult = new DecimalResult() {

		@Override
		public void result(long m, byte e) {
			//users of this instance have already moved default counts forward.
			defaultDecimalM[defaultsCount-1]=m;
			defaultDecimalE[defaultsCount-1]=e;
		}
		
	};
	
	public void defaultRational(TrieParserReader reader, String key, long numerator, long denominator) {
		//if key is not found continue
		if (-1 == reader.query(runtimeParser, key)) {
			//get next index
			int defaultValueIndex = defaultsCount++;
			//grow if needed
			growDefaults(defaultsCount);
			
			defaultIntegers[defaultValueIndex] = numerator/denominator;
			
			Decimal.fromRational(numerator, denominator, recordDecimalResult); 
			
			defaultText[defaultValueIndex] = Appendables.appendValue(Appendables.appendValue(new StringBuilder(), numerator), "/", denominator);
			setBytes(defaultValueIndex);
			
			defaultNumerator[defaultValueIndex] = numerator;
			defaultDenominator[defaultValueIndex] = denominator;
			
			defaultDouble[defaultValueIndex] = (double)numerator/(double)denominator;
		
			//add to map key bytes and the index with mask.
			runtimeParser.setUTF8Value(key, DEFAULT_VALUE_FLAG|defaultValueIndex);
		}
	}
	
	private static final int initialSize = 4;
	
	private long[] defaultIntegers = new long[initialSize];
	private long[] defaultDecimalM = new long[initialSize];
	private byte[] defaultDecimalE = new byte[initialSize];
	private CharSequence[] defaultText = new CharSequence[initialSize];
	private byte[][] defaultBytes = new byte[initialSize][];
	private long[] defaultNumerator = new long[initialSize];
	private long[] defaultDenominator = new long[initialSize];
	private double[] defaultDouble = new double[initialSize];
	
	public long getDefaultInteger(int id) {
		return defaultIntegers[DEFAULT_VALUE_FLAG_MASK&id];
	}
	public long getDefaultDecimalM(int id) {
		return defaultDecimalM[DEFAULT_VALUE_FLAG_MASK&id];
	}
	public byte getDefaultDecimalE(int id) {
		return defaultDecimalE[DEFAULT_VALUE_FLAG_MASK&id];
	}
	public <A extends Appendable> A appendDefaultText(int id, A target) {
		
		if (target instanceof AppendableBuilder) {//TODO: revist this may now be an interface.
			((AppendableBuilder)target).append(defaultBytes[DEFAULT_VALUE_FLAG_MASK&id]);
			return target;
		}
		
		try { 
			target.append(defaultText[DEFAULT_VALUE_FLAG_MASK&id]);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return target;
	}
	public long getDefaultNumerator(int id) {
		return defaultNumerator[DEFAULT_VALUE_FLAG_MASK&id];
	}
	public long getDefaultDenominator(int id) {
		return defaultDenominator[DEFAULT_VALUE_FLAG_MASK&id];
	}
	public double getDefaultDouble(int id) {
		return defaultDouble[DEFAULT_VALUE_FLAG_MASK&id];
	}
	

	public boolean isEqualDefaultText(int id, byte[] equalText) {		
		return Arrays.equals(equalText, defaultBytes[DEFAULT_VALUE_FLAG_MASK&id]);
	}

	public long parse(int id, TrieParserReader reader, TrieParser trie) {
		
	    byte[] bs = defaultBytes[DEFAULT_VALUE_FLAG_MASK&id]; 
		return TrieParserReader.query(reader, trie, bs, 0, bs.length, Integer.MAX_VALUE);

	}
	
	
	private void growDefaults(int len) {
		if (defaultIntegers.length<len) {
			int newLen = len*2;
			
			defaultIntegers = growLong(defaultIntegers, newLen);
			defaultDecimalM = growLong(defaultDecimalM, newLen);
			defaultNumerator = growLong(defaultNumerator, newLen);
			defaultDenominator = growLong(defaultDenominator, newLen);
			defaultText = growText(defaultText, newLen);
			defaultBytes = growBytes(defaultBytes, newLen);
			defaultDecimalE = growByte(defaultDecimalE, newLen);
			defaultDouble = growDouble(defaultDouble, newLen);
			
		}
	}

	private byte[][] growBytes(byte[][] source, int newLen) {
		byte[][] result = new byte[newLen][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}
	
	private double[] growDouble(double[] source, int newLen) {
		double[] result = new double[newLen];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private byte[] growByte(byte[] source, int newLen) {		
		byte[] result = new byte[newLen];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private CharSequence[] growText(CharSequence[] source, int newLen) {
		CharSequence[] result = new CharSequence[newLen];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private long[] growLong(long[] source, int newLen) {
		long[] result = new long[newLen];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}


}
