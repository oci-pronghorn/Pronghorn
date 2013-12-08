package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderDecimal {
	
	private PrimitiveReader reader;
	private final FieldReaderInteger exponent;
	private final FieldReaderLong mantissa;
		
	
	public FieldReaderDecimal(PrimitiveReader reader, int[] decimalExponentDictionary, long[] decimalMantissaDictionary) {
		this.reader = reader;
		this.exponent = new FieldReaderInteger(reader, decimalExponentDictionary);
		this.mantissa = new FieldReaderLong(reader, decimalMantissaDictionary);
	}

	public int readDecimalExponentOptional(int token, int oppExp, int valueOfOptional) {
		switch (oppExp) {
			case OperatorMask.None:
				return exponent.readIntegerSignedOptional(token,valueOfOptional);
			case OperatorMask.Copy:
				return exponent.readIntegerSignedCopyOptional(token,valueOfOptional);
			case OperatorMask.Default:
				return exponent.readIntegerSignedDefaultOptional(token, valueOfOptional);
			case OperatorMask.Delta:
				return exponent.readIntegerSignedDeltaOptional(token, valueOfOptional);
			case OperatorMask.Increment:
				return exponent.readIntegerSignedIncrementOptional(token, valueOfOptional);
		    default:
				throw new UnsupportedOperationException();
		}
	}

	public int readDecimalExponent(int token, int oppExp, int valueOfOptional) {
		switch (oppExp) {
			case OperatorMask.None:
				return exponent.readIntegerSigned(token);
			case OperatorMask.Constant:
				return exponent.readIntegerSignedConstant(token);
			case OperatorMask.Copy:
				return exponent.readIntegerSignedCopy(token);
			case OperatorMask.Default:
				return exponent.readIntegerSignedDefault(token);
			case OperatorMask.Delta:
				return exponent.readIntegerSignedDelta(token);
			case OperatorMask.Increment:
				return exponent.readIntegerSignedIncrement(token);
		    default:
				throw new UnsupportedOperationException();
		}
	}

	public long readDecimalMantissaOptional(int token, int oppMant, long valueOfOptional) {
		switch (oppMant) {
			case OperatorMask.None:
				return mantissa.readLongSignedOptional(token,valueOfOptional);
			case OperatorMask.Copy:
				return mantissa.readLongSignedCopyOptional(token,valueOfOptional);
			case OperatorMask.Default:
				return mantissa.readLongSignedDefaultOptional(token, valueOfOptional);
			case OperatorMask.Delta:
				return mantissa.readLongSignedDeltaOptional(token, valueOfOptional);
			case OperatorMask.Increment:
				return mantissa.readLongSignedIncrementOptional(token, valueOfOptional);
		    default:
				throw new UnsupportedOperationException();
		}
	}

	public long readDecimalMantissa(int token, int oppMant, long valueOfOptional) {
		switch (oppMant) {
			case OperatorMask.None:
				return mantissa.readLongSigned(token);
			case OperatorMask.Constant:
				return mantissa.readLongSignedConstant(token);
			case OperatorMask.Copy:
				return mantissa.readLongSignedCopy(token);
			case OperatorMask.Default:
				return mantissa.readLongSignedDefault(token);
			case OperatorMask.Delta:
				return mantissa.readLongSignedDelta(token);
			case OperatorMask.Increment:
				return mantissa.readLongSignedIncrement(token);
		    default:
				throw new UnsupportedOperationException();
		}
	}

}
