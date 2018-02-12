package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.math.Decimal;
import com.ociweb.pronghorn.util.math.DecimalResult;

public class FieldExtractionDefinitions {

	private final TrieParser runtimeParser;
	private int indexCount;
	public final int groupId;
	public final int pathId;
	public int defaultsCount = 0;
	private static final int DEFAULT_MASK_FLAG = 1<<30;
	
	public FieldExtractionDefinitions(boolean trustText, int groupId, int pathId) {
		this.runtimeParser = new TrieParser(64, 2, trustText, true);
		this.groupId = groupId;
		this.pathId = pathId;

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


	public void defaultText(TrieParserReader reader, String key, String value) {
		//if key is not found continue
		if (-1 == reader.query(runtimeParser, key)) {
			//get next index
			int defaultValueIndex = defaultsCount++;
			//grow if needed
			growDefaults(defaultsCount);
			
			defaultText[defaultValueIndex] = value;
			
			long id = TrieParserReader.query(reader, 
					                         DataInputBlobReader.textToNumberTrieParser(), 
					                         value);
						
			if (id>=0) {
			
				defaultIntegers[defaultValueIndex] = TrieParserReader.capturedLongField(reader, 0);
				
				defaultDecimalM[defaultValueIndex] = TrieParserReader.capturedDecimalMField(reader, 0); 
				defaultDecimalE[defaultValueIndex] = TrieParserReader.capturedDecimalEField(reader, 0);
				
				defaultDouble[defaultValueIndex] = Decimal.asDouble(
													defaultDecimalM[defaultValueIndex], 
													defaultDecimalE[defaultValueIndex]);
				
				defaultNumerator[defaultValueIndex] = 0;						
				defaultDenominator[defaultValueIndex] = 0;
						
				
				//TODO: add fraction support as well??
				
				//TODO: add special logic for both text formats to set double
				
			} else {
				//not a recognized number so do not set			
			}
			
								
			//add to map key bytes and the index with mask.
			runtimeParser.setUTF8Value(key, DEFAULT_MASK_FLAG|defaultValueIndex);
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
			
			defaultNumerator[defaultValueIndex] = value;
			defaultDenominator[defaultValueIndex] = 1;
			
			defaultDecimalM[defaultValueIndex] = value;
			defaultDecimalE[defaultValueIndex] = 0;
			
			defaultDouble[defaultValueIndex] = value;
			
			//add to map key bytes and the index with mask.
			runtimeParser.setUTF8Value(key, DEFAULT_MASK_FLAG|defaultValueIndex);
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
			
			defaultDecimalM[defaultValueIndex] = m;
			defaultDecimalE[defaultValueIndex] = e;
			
			defaultDouble[defaultValueIndex] = Decimal.asDouble(m, e);
									
			//add to map key bytes and the index with mask.
			runtimeParser.setUTF8Value(key, DEFAULT_MASK_FLAG|defaultValueIndex);
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

			defaultNumerator[defaultValueIndex] = numerator;
			defaultDenominator[defaultValueIndex] = denominator;
			
			defaultDouble[defaultValueIndex] = (double)numerator/(double)denominator;
		
			//add to map key bytes and the index with mask.
			runtimeParser.setUTF8Value(key, DEFAULT_MASK_FLAG|defaultValueIndex);
		}
	}
	
	private static final int initialSize = 4;
	private long[] defaultIntegers = new long[initialSize];
	private long[] defaultDecimalM = new long[initialSize];
	private byte[] defaultDecimalE = new byte[initialSize];
	private CharSequence[] defaultText = new CharSequence[initialSize];
	private long[] defaultNumerator = new long[initialSize];
	private long[] defaultDenominator = new long[initialSize];
	private double[] defaultDouble = new double[initialSize];
	
	
	private void growDefaults(int len) {
		if (defaultIntegers.length<len) {
			int newLen = len*2;
			
			defaultIntegers = growLong(defaultIntegers, newLen);
			defaultDecimalM = growLong(defaultDecimalM, newLen);
			defaultNumerator = growLong(defaultNumerator, newLen);
			defaultDenominator = growLong(defaultDenominator, newLen);
			defaultText = growText(defaultText, newLen);
			defaultDecimalE = growByte(defaultDecimalE, newLen);
			defaultDouble = growDouble(defaultDouble, newLen);
			
		}
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
