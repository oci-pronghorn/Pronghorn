package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldWriterDecimal {

	private final FieldWriterInteger writerDecimalExponent;
	private final FieldWriterLong writerDecimalMantissa;
	
	public FieldWriterDecimal(PrimitiveWriter writer, int[] exponentValues, long[] mantissaValues) {
		writerDecimalExponent = new FieldWriterInteger(writer, exponentValues);
		writerDecimalMantissa = new FieldWriterLong(writer, mantissaValues);
	}

	public void writeDecimalConstant(int token, int exponent, long mantissa) {

		writerDecimalExponent.writeIntegerSignedConstant(exponent, token);
		writerDecimalMantissa.writeLongSignedConstant(mantissa, token);
				
	}

	public void writeDecimalNone(int token, int exponent, long mantissa) {
		
		writerDecimalExponent.writeIntegerSigned(exponent, token);
		writerDecimalMantissa.writeLongSigned(mantissa, token);
		
	}

	public void writeDecimalOptional(int token, int oppExp, int oppMant, int exponent, long mantissa) {
		switch(oppExp) {
			case OperatorMask.None:
				writerDecimalExponent.writeIntegerSigned(1+exponent, token);
			break;
			case OperatorMask.Copy:
				writerDecimalExponent.writeIntegerSignedCopyOptional(exponent, token);
			break;
			case OperatorMask.Default:
				writerDecimalExponent.writeIntegerSignedDefaultOptional(exponent, token);
			break;
			case OperatorMask.Delta:
				writerDecimalExponent.writeIntegerSignedDeltaOptional(exponent, token);
			break;	
			case OperatorMask.Increment:
				writerDecimalExponent.writeIntegerSignedIncrementOptional(exponent, token);
			break;
			default:
				throw new UnsupportedOperationException();
		}
		
		switch(oppMant) {
			case OperatorMask.None:
				writerDecimalMantissa.writeLongSigned(1+mantissa, token);
			break;
			case OperatorMask.Copy:
				writerDecimalMantissa.writeLongSignedCopyOptional(mantissa, token);
			break;
			case OperatorMask.Default:
				writerDecimalMantissa.writeLongSignedDefaultOptional(mantissa, token);
			break;
			case OperatorMask.Delta:
				writerDecimalMantissa.writeLongSignedDeltaOptional(mantissa, token);
			break;	
			case OperatorMask.Increment:
				writerDecimalMantissa.writeLongSignedIncrementOptional(mantissa, token);
			break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	public void writeDecimal(int token, int oppExp, int oppMant, int exponent, long mantissa) {
		switch(oppExp) {
			case OperatorMask.None:
				writerDecimalExponent.writeIntegerSigned(exponent, token);
			break;
			case OperatorMask.Constant:
				writerDecimalExponent.writeIntegerSignedConstant(exponent, token);
			break;
			case OperatorMask.Copy:
				writerDecimalExponent.writeIntegerSignedCopy(exponent, token);
			break;
			case OperatorMask.Default:
				writerDecimalExponent.writeIntegerSignedDefault(exponent, token);
			break;
			case OperatorMask.Delta:
				writerDecimalExponent.writeIntegerSignedDelta(exponent, token);
			break;	
			case OperatorMask.Increment:
				writerDecimalExponent.writeIntegerSignedIncrement(exponent, token);
			break;
		}
		
		switch(oppMant) {
			case OperatorMask.None:
				writerDecimalMantissa.writeLongSigned(mantissa, token);
			break;
			case OperatorMask.Constant:
				writerDecimalMantissa.writeLongSignedConstant(mantissa, token);
			break;
			case OperatorMask.Copy:
				writerDecimalMantissa.writeLongSignedCopy(mantissa, token);
			break;
			case OperatorMask.Default:
				writerDecimalMantissa.writeLongSignedDefault(mantissa, token);
			break;
			case OperatorMask.Delta:
				writerDecimalMantissa.writeLongSignedDelta(mantissa, token);
			break;	
			case OperatorMask.Increment:
				writerDecimalMantissa.writeLongSignedIncrement(mantissa, token);
			break;
		}
	}
}
