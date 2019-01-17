package com.ociweb.pronghorn.network.http;

import java.util.Arrays;

import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.struct.ByteSequenceValidator;
import com.ociweb.pronghorn.struct.DecimalValidator;
import com.ociweb.pronghorn.struct.LongValidator;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.CharSequenceToUTF8Local;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;
import com.ociweb.pronghorn.util.math.Decimal;

public class FieldExtractionDefinitions {

	private final TrieParser fieldParamParser;
	private int indexCount;
	public final int structId;
	public final int routeId;
	public final int pathId;
	public int defaultsCount = 0;
	//private transient Pipe<RawDataSchema> workingPipe = null;
		
	//private static final TrieParser numberParser = textToNumberTrieParser();
		
	private DefaultArgPopulation populateDefaults = new DefaultArgPopulation();

	public FieldExtractionDefinitions(boolean trustText, int routeId, int pathId, int structId) {
		
		//field name to type and index
		this.fieldParamParser = new TrieParser(64, 4, trustText, true);
		this.routeId = routeId;
		this.pathId = pathId;
		this.structId = structId;
	}

	public int routeId() {
		return routeId;
	}
	
	private int[] pathFieldLookup;
	private Object[] pathFieldValidator;
	
	public void setPathFieldLookup(int[] pathFieldLookup, Object[] validators) {
		//fields found in the path
		this.pathFieldLookup = pathFieldLookup;
		this.pathFieldValidator = validators;
		
//		new Exception("set index lookup values "+Arrays.toString(pathFieldLookup)).printStackTrace();

	}
	
	public int[] paramIndexArray() {
		return pathFieldLookup;
	}
	
	public Object[] paramIndexArrayValidator() {
		return pathFieldValidator;
	}

//	private static TrieParser textToNumberTrieParser() {
//		 TrieParser p = new TrieParser(8,true); //supports only complete values
//		 p.setUTF8Value("%i%.%/%.", 1); 
//		 return p;
//	}
	
	public TrieParser getFieldParamParser() {
		return fieldParamParser;
	}
	
	public void setIndexCount(int indexCount) {
		this.indexCount = indexCount;
	}
	
	public int getIndexCount() {
		return this.indexCount;
	}


	public void defaultText(byte[] key, final String value, StructRegistry registry) {
		
		//only add this as a default if it is not already included in the url path
		if (-1 != TrieParserReader.query(TrieParserReaderLocal.get(), fieldParamParser, key, 0, key.length, Integer.MAX_VALUE)) {
			return;
		}		
		
		long fieldId = registry.fieldLookup(key, 0, key.length, Integer.MAX_VALUE, structId);
		Object validator = registry.fieldValidator(fieldId);
		
		final DefaultArgPopulation prev = populateDefaults;
		final int fieldPos = StructRegistry.extractFieldPosition(fieldId);
			
		switch (registry.fieldType(fieldId)) {
			case Text:				
				final byte[] textBytes = CharSequenceToUTF8Local.get().convert(value).asBytes();
								
				if (validator instanceof ByteSequenceValidator) {	
					if (!((ByteSequenceValidator)validator).isValid(textBytes, 0, textBytes.length, Integer.MAX_VALUE)) {
						throw new UnsupportedOperationException("Default value "+value+" does not pass validation.");
					}
				}
				populateDefaults = new DefaultArgPopulation() {
					public void populate(DataOutputBlobWriter<?> writer) {				
						int location = writer.position();				
						DataOutputBlobWriter.setIntBackData(writer, location, fieldPos);
						writer.writeShort(textBytes.length);
						writer.write(textBytes);			
						prev.populate(writer);
					}
				};
			break;
			case Long:  
			case Integer: 
			case Short: 
			case Boolean:
			case Byte:
			case Decimal:
			case Double:
			case Float:
			case Rational:
			case Blob: 
			default:
				throw new UnsupportedOperationException("type mismatch");			
		}			
		
	}

	
	public void defaultInteger(byte[] key, final long value, StructRegistry registry) {
				
		//only add this as a default if it is not already included in the url path
		if (-1 != TrieParserReader.query(TrieParserReaderLocal.get(), fieldParamParser, key, 0, key.length, Integer.MAX_VALUE)) {
			return;
		}
		
		long fieldId = registry.fieldLookup(key, 0, key.length, Integer.MAX_VALUE, structId);
		Object validator = registry.fieldValidator(fieldId);
		
		final DefaultArgPopulation prev = populateDefaults;
		final int fieldPos = StructRegistry.extractFieldPosition(fieldId);
			
		switch (registry.fieldType(fieldId)) {
			case Long:  
			case Integer: 
			case Short: 
				if (validator instanceof LongValidator) {
					if (!((LongValidator)validator).isValid(value)) {
						throw new UnsupportedOperationException("Default value "+value+" does not pass validation.");
					}
				} 
				populateDefaults = new DefaultArgPopulation() {
					public void populate(DataOutputBlobWriter<?> writer) {				
						int location = writer.position();				
						DataOutputBlobWriter.setIntBackData(writer, location, fieldPos);
						writer.writePackedLong(value);				
						prev.populate(writer);
					}
				};
			break;
			case Text:
				StringBuilder text = Appendables.appendValue(new StringBuilder(), value);				
				final byte[] textBytes = CharSequenceToUTF8Local.get().convert(text).asBytes();
								
				if (validator instanceof ByteSequenceValidator) {	
					if (!((ByteSequenceValidator)validator).isValid(textBytes, 0, textBytes.length, Integer.MAX_VALUE)) {
						throw new UnsupportedOperationException("Default value "+text+" does not pass validation.");
					}
				}
				populateDefaults = new DefaultArgPopulation() {
					public void populate(DataOutputBlobWriter<?> writer) {				
						int location = writer.position();				
						DataOutputBlobWriter.setIntBackData(writer, location, fieldPos);
						writer.writeShort(textBytes.length);
						writer.write(textBytes);			
						prev.populate(writer);
					}
				};
			break;
			case Decimal:
				final long m = value;
				final byte e = 0;
				if (validator instanceof DecimalValidator) {
					if (!((DecimalValidator)validator).isValid(m, e)) {
						throw new UnsupportedOperationException("Default value "+Appendables.appendDecimalValue(new StringBuilder(), m, e)+" does not pass validation.");
					}
				}
				populateDefaults = new DefaultArgPopulation() {
					public void populate(DataOutputBlobWriter<?> writer) {				
						int location = writer.position();				
						DataOutputBlobWriter.setIntBackData(writer, location, fieldPos);
						writer.writePackedLong(m);		
						writer.writeByte(e);
						prev.populate(writer);
					}
				};
			break;
			case Boolean:
			case Byte:
			case Double:
			case Float:
			case Rational:
			case Blob: 
			default:
				throw new UnsupportedOperationException("type mismatch");			
		}			
	}
	
	public void defaultDecimal(byte[] key, final long m, final byte e, StructRegistry registry) {
		
		//only add this as a default if it is not already included in the url path
		if (-1 != TrieParserReader.query(TrieParserReaderLocal.get(), fieldParamParser, key, 0, key.length, Integer.MAX_VALUE)) {
			return;
		}
		
		long fieldId = registry.fieldLookup(key, 0, key.length, Integer.MAX_VALUE, structId);
		Object validator = registry.fieldValidator(fieldId);

		final DefaultArgPopulation prev = populateDefaults;
		final int fieldPos = StructRegistry.extractFieldPosition(fieldId);
			
		switch (registry.fieldType(fieldId)) {

			case Text:
				StringBuilder text = Appendables.appendDecimalValue(new StringBuilder(), m, e);				
				final byte[] textBytes = CharSequenceToUTF8Local.get().convert(text).asBytes();
								
				if (validator instanceof ByteSequenceValidator) {	
					if (!((ByteSequenceValidator)validator).isValid(textBytes, 0, textBytes.length, Integer.MAX_VALUE)) {
						throw new UnsupportedOperationException("Default value "+text+" does not pass validation.");
					}
				}
				populateDefaults = new DefaultArgPopulation() {
					public void populate(DataOutputBlobWriter<?> writer) {				
						int location = writer.position();				
						DataOutputBlobWriter.setIntBackData(writer, location, fieldPos);
						writer.writeShort(textBytes.length);
						writer.write(textBytes);			
						prev.populate(writer);
					}
				};
			break;	
			case Decimal:
				if (validator instanceof DecimalValidator) {
					if (!((DecimalValidator)validator).isValid(m, e)) {
						throw new UnsupportedOperationException("Default value "+Appendables.appendDecimalValue(new StringBuilder(), m, e)+" does not pass validation.");
					}
				}
				populateDefaults = new DefaultArgPopulation() {
					public void populate(DataOutputBlobWriter<?> writer) {				
						int location = writer.position();				
						DataOutputBlobWriter.setIntBackData(writer, location, fieldPos);
						writer.writePackedLong(m);		
						writer.writeByte(e);
						prev.populate(writer);
					}
				};
			break;
			case Long:  
			case Integer: 
			case Short:
				final long longValue = Decimal.asLong(m, e);				
				if (validator instanceof LongValidator) {
					if (!((LongValidator)validator).isValid(longValue)) {
						throw new UnsupportedOperationException("Default value "+longValue+" does not pass validation.");
					}
				}
				populateDefaults = new DefaultArgPopulation() {
					public void populate(DataOutputBlobWriter<?> writer) {				
						int location = writer.position();				
						DataOutputBlobWriter.setIntBackData(writer, location, fieldPos);
						writer.writePackedLong(longValue);				
						prev.populate(writer);
					}
				};				
			break;	
			case Boolean:
			case Byte:
			case Double:
			case Float:
			case Rational:
			case Blob: 
			default:
				throw new UnsupportedOperationException("type mismatch");			
		}
	}


	
	public void defaultRational(byte[] key, long numerator, long denominator, StructRegistry registry) {

		//only add this as a default if it is not already included in the url path
		if (-1 != TrieParserReader.query(TrieParserReaderLocal.get(), fieldParamParser, key, 0, key.length, Integer.MAX_VALUE)) {
			return;
		}
		
		long fieldId = registry.fieldLookup(key, 0, key.length, Integer.MAX_VALUE, structId);
		Object validator = registry.fieldValidator(fieldId);

		final DefaultArgPopulation prev = populateDefaults;
		final int fieldPos = StructRegistry.extractFieldPosition(fieldId);
			
		switch (registry.fieldType(fieldId)) {
			case Long:  
			case Integer: 
			case Short:
				final long intValue = numerator/denominator;
				if (validator instanceof LongValidator) {
					if (!((LongValidator)validator).isValid(intValue)) {
						throw new UnsupportedOperationException("Default value "+intValue+" does not pass validation.");
					}
				}
				populateDefaults = new DefaultArgPopulation() {
					public void populate(DataOutputBlobWriter<?> writer) {				
						int location = writer.position();				
						DataOutputBlobWriter.setIntBackData(writer, location, fieldPos);
						writer.writePackedLong(intValue);				
						prev.populate(writer);
					}
				};
			break;
			case Text:
				StringBuilder text = Appendables.appendValue(Appendables.appendValue(new StringBuilder(), numerator), "/", denominator);				
				final byte[] textBytes = CharSequenceToUTF8Local.get().convert(text).asBytes();
								
				if (validator instanceof ByteSequenceValidator) {	
					if (!((ByteSequenceValidator)validator).isValid(textBytes, 0, textBytes.length, Integer.MAX_VALUE)) {
						throw new UnsupportedOperationException("Default value "+text+" does not pass validation.");
					}
				}
				populateDefaults = new DefaultArgPopulation() {
					public void populate(DataOutputBlobWriter<?> writer) {				
						int location = writer.position();				
						DataOutputBlobWriter.setIntBackData(writer, location, fieldPos);
						writer.writeShort(textBytes.length);
						writer.write(textBytes);			
						prev.populate(writer);
					}
				};
			break;				
			case Boolean:
			case Byte:
			case Decimal:
			case Double:
			case Float:
			case Rational:
			case Blob: 
			default:
				throw new UnsupportedOperationException("type mismatch");			
		}
	}


	public void processDefaults(DataOutputBlobWriter<HTTPRequestSchema> writer) {

		populateDefaults.populate(writer);
		
		//TODO: upon contruction confirm that null can not happen in the path if the validator requries it..
		//if we have 1 path missing some args and we have no default for that arg, then we need to run validator.		
		
	}


}
