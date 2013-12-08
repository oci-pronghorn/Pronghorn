package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.FieldReaderBytes;
import com.ociweb.jfast.field.FieldReaderChar;
import com.ociweb.jfast.field.FieldReaderDecimal;
import com.ociweb.jfast.field.FieldReaderInteger;
import com.ociweb.jfast.field.FieldReaderLong;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;

//May drop interface if this causes a performance problem from virtual table
public class FASTStaticReader implements FASTReader {

	private final PrimitiveReader reader;
	private final int[] tokenLookup; //array of tokens as field id locations
	
	//package protected so DynamicReader can use these instances
	private final FieldReaderInteger readerInteger;
	private final FieldReaderLong    readerLong;
	private final FieldReaderDecimal readerDecimal;
	private final FieldReaderChar readerChar;
	private final FieldReaderBytes readerBytes;
	

	
		
	public FASTStaticReader(PrimitiveReader reader, DictionaryFactory dcr, int[] tokenLookup) {
		this.reader=reader;
		this.tokenLookup = tokenLookup;
		
		this.readerInteger = new FieldReaderInteger(reader,dcr.integerDictionary());
		this.readerLong = new FieldReaderLong(reader,dcr.longDictionary());
		this.readerDecimal = new FieldReaderDecimal(reader, dcr.decimalExponentDictionary(),dcr.decimalMantissaDictionary());
		this.readerBytes = null;
		this.readerChar = null;
		
		
		
	}
	

	public void reset(DictionaryFactory df) {
		//clear all previous values to unset
		readerInteger.reset(df);
		readerLong.reset(df);
	}

	
	//package protected, unless we find a need to expose it?
	void readToken(int token) {
		//used by groups which hold list of tokens
		//at end of each group call back may be done and FASTDynamicReader used.

	    switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.IntegerUnsigned:
				readIntegerUnsigned(token);
				break;
			case TypeMask.IntegerUnsignedOptional:
				readIntegerUnsignedOptional(token,0);
				break;
			case TypeMask.IntegerSigned:
				readIntegerSigned(token);
				break;
			case TypeMask.IntegerSignedOptional:
				readIntegerSignedOptional(token,0);
				break;
			case TypeMask.LongUnsigned:
				readLongUnsigned(token);
				break;
			case TypeMask.LongUnsignedOptional:
				readLongUnsignedOptional(token,0);
				break;
			case TypeMask.LongSigned:
				readLongSigned(token);
				break;
			case TypeMask.LongSignedOptional:
				readLongSignedOptional(token,0);
				break;
			case TypeMask.TextASCII:
				readTextASCII(token, null); //TODO: these nulls are not corect but we do not need the result.
			    break;
		    case TypeMask.TextASCIIOptional:
				readTextASCIIOptional(token, null);
			    break;
			case TypeMask.TextUTF8:
				readTextUTF8(token,null);
				break;
			case TypeMask.TextUTF8Optional:
				readTextUTF8Optional(token, null);
				break;
			case TypeMask.Decimal:
				//readerDecimal();
				break;
			case TypeMask.DecimalOptional:
				break;
			case TypeMask.ByteArray:
				break;
			case TypeMask.ByteArrayOptional:
				break;
			default:
				throw new UnsupportedOperationException();
			}
	}
	
	
	@Override
	public long readLong(int id, long valueOfOptional) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.LongUnsigned:
				return readLongUnsigned(token);
			case TypeMask.LongUnsignedOptional:
				return readLongUnsignedOptional(token, valueOfOptional);
			case TypeMask.LongSigned:
				return readLongSigned(token);
			case TypeMask.LongSignedOptional:
				return readLongSignedOptional(token, valueOfOptional);
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}
	
	private long readLongSignedOptional(int token, long valueOfOptional) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
		case OperatorMask.None:
			return readerLong.readLongSignedOptional(token,valueOfOptional);
		case OperatorMask.Copy:
			return readerLong.readLongSignedCopyOptional(token,valueOfOptional);
		case OperatorMask.Default:
			return readerLong.readLongSignedDefaultOptional(token,valueOfOptional);
		case OperatorMask.Delta:
			return readerLong.readLongSignedDeltaOptional(token,valueOfOptional);
		case OperatorMask.Increment:
			return readerLong.readLongSignedIncrementOptional(token,valueOfOptional);	
		default:
			throw new UnsupportedOperationException();
		}
	}

	private long readLongSigned(int token) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
		case OperatorMask.None:
			return readerLong.readLongSigned(token);
		case OperatorMask.Constant:
			return readerLong.readLongSignedConstant(token);
		case OperatorMask.Copy:
			return readerLong.readLongSignedCopy(token);
		case OperatorMask.Default:
			return readerLong.readLongSignedDefault(token);
		case OperatorMask.Delta:
			return readerLong.readLongSignedDelta(token);
		case OperatorMask.Increment:
			return readerLong.readLongSignedIncrement(token);		
		default:
			throw new UnsupportedOperationException();
		}
	}

	private long readLongUnsignedOptional(int token, long valueOfOptional) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				return readerLong.readLongUnsignedOptional(token,valueOfOptional);
			case OperatorMask.Copy:
				return readerLong.readLongUnsignedCopyOptional(token,valueOfOptional);
			case OperatorMask.Default:
				return readerLong.readLongUnsignedDefaultOptional(token,valueOfOptional);
			case OperatorMask.Delta:
				return readerLong.readLongUnsignedDeltaOptional(token,valueOfOptional);
			case OperatorMask.Increment:
				return readerLong.readLongUnsignedIncrementOptional(token,valueOfOptional);	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private long readLongUnsigned(int token) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				return readerLong.readLongUnsigned(token);
			case OperatorMask.Constant:
				return readerLong.readLongUnsignedConstant(token);
			case OperatorMask.Copy:
				return readerLong.readLongUnsignedCopy(token);
			case OperatorMask.Default:
				return readerLong.readLongUnsignedDefault(token);
			case OperatorMask.Delta:
				return readerLong.readLongUnsignedDelta(token);
			case OperatorMask.Increment:
				return readerLong.readLongUnsignedIncrement(token);		
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public int readInt(int id, int valueOfOptional) {
		
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.IntegerUnsigned:
				return readIntegerUnsigned(token);
			case TypeMask.IntegerUnsignedOptional:
				return readIntegerUnsignedOptional(token, valueOfOptional);
			case TypeMask.IntegerSigned:
				return readIntegerSigned(token);
			case TypeMask.IntegerSignedOptional:
				return readIntegerSignedOptional(token, valueOfOptional);
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
		
	}

	private int readIntegerSignedOptional(int token, int valueOfOptional) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
		case OperatorMask.None:
			return readerInteger.readIntegerSignedOptional(token,valueOfOptional);
		case OperatorMask.Copy:
			return readerInteger.readIntegerSignedCopyOptional(token,valueOfOptional);
		case OperatorMask.Default:
			return readerInteger.readIntegerSignedDefaultOptional(token,valueOfOptional);
		case OperatorMask.Delta:
			return readerInteger.readIntegerSignedDeltaOptional(token,valueOfOptional);
		case OperatorMask.Increment:
			return readerInteger.readIntegerSignedIncrementOptional(token,valueOfOptional);	
		default:
			throw new UnsupportedOperationException();
		}
	}

	private int readIntegerSigned(int token) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
		case OperatorMask.None:
			return readerInteger.readIntegerSigned(token);
		case OperatorMask.Constant:
			return readerInteger.readIntegerSignedConstant(token);
		case OperatorMask.Copy:
			return readerInteger.readIntegerSignedCopy(token);
		case OperatorMask.Default:
			return readerInteger.readIntegerSignedDefault(token);
		case OperatorMask.Delta:
			return readerInteger.readIntegerSignedDelta(token);
		case OperatorMask.Increment:
			return readerInteger.readIntegerSignedIncrement(token);		
		default:
			throw new UnsupportedOperationException();
		}
	}

	private int readIntegerUnsignedOptional(int token, int valueOfOptional) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				return readerInteger.readIntegerUnsignedOptional(token,valueOfOptional);
			case OperatorMask.Copy:
				return readerInteger.readIntegerUnsignedCopyOptional(token,valueOfOptional);
			case OperatorMask.Default:
				return readerInteger.readIntegerUnsignedDefaultOptional(token,valueOfOptional);
			case OperatorMask.Delta:
				return readerInteger.readIntegerUnsignedDeltaOptional(token,valueOfOptional);
			case OperatorMask.Increment:
				return readerInteger.readIntegerUnsignedIncrementOptional(token,valueOfOptional);	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readIntegerUnsigned(int token) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				return readerInteger.readIntegerUnsigned(token);
			case OperatorMask.Constant:
				return readerInteger.readIntegerUnsignedConstant(token);
			case OperatorMask.Copy:
				return readerInteger.readIntegerUnsignedCopy(token);
			case OperatorMask.Default:
				return readerInteger.readIntegerUnsignedDefault(token);
			case OperatorMask.Delta:
				return readerInteger.readIntegerUnsignedDelta(token);
			case OperatorMask.Increment:
				return readerInteger.readIntegerUnsignedIncrement(token);		
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void readBytes(int id, ByteBuffer target) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.ByteArray:
				throw new UnsupportedOperationException();
			case TypeMask.ByteArrayOptional:
				throw new UnsupportedOperationException();
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public int readBytes(int id, byte[] target, int offset) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.ByteArray:
				throw new UnsupportedOperationException();
			case TypeMask.ByteArrayOptional:
				throw new UnsupportedOperationException();
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void openGroup(int id) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		reader.readPMap(TokenBuilder.MASK_PMAP_MAX&(token>>TokenBuilder.SHIFT_PMAP_MASK));
		
	}

	@Override
	public void closeGroup() {
		reader.popPMap();
	}

	public boolean isGroupOpen() {
		return reader.isPMapOpen();
	}

	@Override
	public int readDecimalExponent(int id, int valueOfOptional) {
		int token = id>=0 ? tokenLookup[id] : id;
		int optional = ((token>>TokenBuilder.SHIFT_TYPE)&1);
		int oppExp = (token>>(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL))&TokenBuilder.MASK_OPER_DECIMAL;
		if (0==optional) {
			return readerDecimal.readDecimalExponentOptional(token, oppExp, valueOfOptional);
		} else {
			return readerDecimal.readDecimalExponent(token, oppExp, valueOfOptional);
		}
	}
	

	@Override
	public long readDecimalMantissa(int id, long valueOfOptional) {
		int token = id>=0 ? tokenLookup[id] : id;
		int optional = ((token>>TokenBuilder.SHIFT_TYPE)&1);
		int oppMant = (token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER_DECIMAL;
		if (0==optional) {
			return readerDecimal.readDecimalMantissaOptional(token, oppMant, valueOfOptional);
		} else {
			return readerDecimal.readDecimalMantissa(token, oppMant, valueOfOptional);
		}
	}

	@Override
	public void readChars(int id, Appendable target) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.TextASCII:
					readTextASCII(token, target);
				break;
			case TypeMask.TextASCIIOptional:
					readTextASCIIOptional(token, target);
				break;
			case TypeMask.TextUTF8:
					readTextUTF8(token,target);
				break;
			case TypeMask.TextUTF8Optional:
					readTextUTF8Optional(token, target);
			    break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextUTF8Optional(int token, Appendable target) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned()-1;
				reader.readTextUTF8(length, target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextUTF8(int token, Appendable target) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned();
				reader.readTextUTF8(length, target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextASCIIOptional(int token, Appendable target) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				reader.readTextASCII(target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextASCII(int token, Appendable target) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				reader.readTextASCII(target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public int readChars(int id, char[] target, int offset) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.TextASCII:
				return readTextASCII(token, target, offset);
			case TypeMask.TextASCIIOptional:
				return readTextASCIIOptional(token, target, offset);
			case TypeMask.TextUTF8:
				return readTextUTF8(token, target, offset);
			case TypeMask.TextUTF8Optional:
				return readTextUTF8Optional(token, target, offset);
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readTextUTF8Optional(int token, char[] target, int offset) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned()-1;
				reader.readTextUTF8(target,offset,length);
				return length;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readTextUTF8(int token, char[] target, int offset) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned();
				reader.readTextUTF8(target,offset,length);
				return length;
			default:
				throw new UnsupportedOperationException();
	}
	}

	private int readTextASCIIOptional(int token, char[] target, int offset) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				return reader.readTextASCII(target,offset);
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readTextASCII(int token, char[] target, int offset) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				return reader.readTextASCII(target,offset);
			default:
				throw new UnsupportedOperationException();
		}
	}



}
