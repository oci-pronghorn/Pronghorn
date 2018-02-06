package com.ociweb.pronghorn.pipe;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.OutputStream;

import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.util.ByteConsumer;
import com.ociweb.pronghorn.util.field.StructuredBlobWriter;

public abstract class ChannelWriter extends OutputStream implements ObjectOutput, Appendable, ByteConsumer, StructuredBlobWriter {

	 abstract public void writeUTF(String s);
	 
	 abstract public int remaining();
	 
	 abstract public int length();
	 
	 abstract public byte[] toByteArray();
	 
	 abstract public Appendable append(CharSequence csq);
	 
	 abstract public Appendable append(CharSequence csq, int start, int end);
	 
	 abstract public Appendable append(char c);	 
	 
	 abstract public boolean reportObjectSizes(Appendable target);
	 
	 abstract public void write(Externalizable object);
	 
	 abstract public void writeObject(Object object);
	 
	 abstract public void writeUTF8Text(CharSequence s);
	 
	 abstract public void writeUTF(CharSequence s);
	 
	 abstract public void writeASCII(CharSequence s);
	 	 
	 abstract public void writeRational(long numerator, long denominator);
	 
	 abstract public void writeUTFArray(String[] utfs);
	 
	 abstract public void write(byte[] source, int sourceOff, int sourceLen, int sourceMask);
	 
	 abstract public void writePackedString(CharSequence text);
	 
	 abstract public void writePackedLong(long value);
	 
	 abstract public void writePackedInt(int value);
	 
	 abstract public void writePackedShort(short value);
	 
	 abstract public void writeDouble(double value);
	 
	 abstract public void writeFloat(float value);	 
	 
	 abstract public void writeLong(long v);
	 
	 abstract public void writeInt(int v);
	 
	 abstract public void writeShort(int v);

	 abstract public void writeByte(int v);
	 
	 abstract public void writeBoolean(boolean v);
	 
	 abstract public void writeBooleanNull();

	 abstract public void writePackedNull();
	 
	 abstract public void write(byte b[], int off, int len);
	 
	 abstract public void write(byte b[]);
	 
	 abstract public void writeUTF8Text(CharSequence s, int pos, int len);
	 
		public void writeInt(int fieldId, int value) {
			
			writePackedInt(TokenBuilder.buildToken(TypeMask.IntegerSigned, 
					                                    OperatorMask.Field_None, 
					                                    fieldId));
			writeShort(-1); //room for future field name
		    writePackedInt(value);
			
		}

		public void writeLong(int fieldId, long value) {
			
			writePackedInt(TokenBuilder.buildToken(TypeMask.LongSigned, 
										                OperatorMask.Field_None, 
										                fieldId));
			writeShort(-1); //room for future field name
			writePackedLong(value);
		}

		public void writeBytes(int fieldId, byte[] backing, int offset, int length) {
			
			writePackedInt(TokenBuilder.buildToken(TypeMask.ByteVector, 
					OperatorMask.Field_None, 
					fieldId));
			writeShort(-1); //room for future field name
			writeShort(backing.length);
			write(backing,offset,length);
			
		}

		public void writeBytes(int fieldId, byte[] backing) {
			writePackedInt(TokenBuilder.buildToken(TypeMask.ByteVector, 
					OperatorMask.Field_None, 
					fieldId));
			writeShort(-1); //room for future field name
			writeShort(backing.length);
			write(backing);
		}
		

		public void writeBytes(int fieldId, byte[] backing, int offset, int length, int mask) {
			
			writePackedInt(TokenBuilder.buildToken(TypeMask.ByteVector, 
	                									OperatorMask.Field_None, 
	                									fieldId));
			writeShort(-1); //room for future field name
			writeShort(length);
			write(backing, offset, length, mask);
			
		}

		public void writeUTF8(int fieldId, CharSequence text, int offset, int length) {
			
			writePackedInt(TokenBuilder.buildToken(TypeMask.TextUTF8, 
										                OperatorMask.Field_None, 
										                fieldId));
			writeShort(-1); //room for future field name
			
			writeShort(length);
			
			writeUTF8Text(text, offset, length);

		}

		public void writeUTF8(int fieldId, CharSequence text) {
			
			writePackedInt(TokenBuilder.buildToken(TypeMask.TextUTF8, 
										                OperatorMask.Field_None, 
										                fieldId));
			
			writeShort(-1); //room for future field name		
			writeUTF(text);
		}


		public void writeDecimal(int fieldId, byte e, long m) {
			writePackedInt(TokenBuilder.buildToken(TypeMask.Decimal, 
	                OperatorMask.Field_None, 
	                fieldId));
			writeShort(-1); //room for future field name
			writeByte(e);
			writePackedLong(m);
		}

		public void writeDouble(int fieldId, double value, byte places) {
			
			writePackedInt(TokenBuilder.buildToken(TypeMask.Decimal, 
										                OperatorMask.Field_None, 
										                fieldId));
			writeShort(-1); //room for future field name
			writeByte(-places);
			writePackedLong((long)Math.rint(value*PipeWriter.powd[64+places]));

		}

	
		public void writeFloat(int fieldId, float value, byte places) {
			
			writePackedInt(TokenBuilder.buildToken(TypeMask.Decimal, 
										                OperatorMask.Field_None, 
										                fieldId));
			writeShort(-1); //room for future field name
			writeByte(-places);
			writePackedLong((long)Math.rint(value*PipeWriter.powd[64+places]));
			
		}
		

		public void writeRational(int fieldId, long numerator, long denominator) {
			//NB: the type DecimalOptional is used to indicate rational value since Nulls are never sent
			writePackedInt(TokenBuilder.buildToken(TypeMask.Rational, 
										                OperatorMask.Field_None, 
										                fieldId));
			writeShort(-1); //room for future field name
			writePackedLong(numerator);
			writePackedLong(denominator);
		}

		
}
