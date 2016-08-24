package com.ociweb.pronghorn.components.ingestion.metaMessageUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;

import com.ociweb.pronghorn.pipe.Pipe;


public class MetaMessageWriter  {

    private static void writeNullableMessage(Pipe ring, int msg, boolean isNullable, String columnName, Object value) {
        if (isNullable) {
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
            Pipe.addIntValue((value != null) ? 1 : 0,ring); // true if not null
            Pipe.publishWrites(ring);
        }
    }

    private static void writeNullableIntMessage(Pipe ring, boolean isNullable, boolean isSigned, String columnName, Object value) {
        writeNullableMessage(ring, (!isSigned && (columnName == null)) ? MetaMessageDefs.MSG_NULLABLEUINT32_LOC : (isSigned && (columnName == null)) ? MetaMessageDefs.MSG_NULLABLEINT32_LOC
                : (!isSigned && (columnName != null)) ? MetaMessageDefs.MSG_NAMEDNULLABLEUINT32_LOC : MetaMessageDefs.MSG_NAMEDNULLABLEINT32_LOC, isNullable, columnName, value);
    }

    private static void writeIntMessage(Pipe ring, boolean isNullable, boolean isSigned, String columnName, int value) {
        // int is written as int32/uint32
        int msg = (!isSigned && (columnName == null)) ? MetaMessageDefs.MSG_UINT32_LOC : (isSigned && (columnName == null)) ? MetaMessageDefs.MSG_INT32_LOC : (!isSigned && (columnName != null)) ? MetaMessageDefs.MSG_NAMEDUINT32_LOC
                : MetaMessageDefs.MSG_NAMEDINT32_LOC;

        Pipe.blockWriteMessage(ring, msg);
        if (columnName != null) {
            Pipe.validateVarLength(ring, columnName.length());
			int sourceLen = columnName.length();
			final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
			Pipe.addBytePosAndLen(ring, p, sourceLen);
        }
        Pipe.addIntValue(value, ring);
        Pipe.publishWrites(ring);
    }

    public static void writeByteMessage(Pipe ring, boolean isNullable, boolean isSigned, String columnName, Object value) {
        // byte is written as int32/uint32
        writeNullableIntMessage(ring, isNullable, isSigned, columnName, value);
        if (value != null)
            writeIntMessage(ring, isNullable, isSigned, columnName, ((Number)value).byteValue());
    }

    public static void writeShortMessage(Pipe ring, boolean isNullable, boolean isSigned, String columnName, Object value) {
        // short is written as int32/uint32
        writeNullableIntMessage(ring, isNullable, isSigned, columnName, value);
        if (value != null)
            writeIntMessage(ring, isNullable, isSigned, columnName, ((Number)value).shortValue());
    }

    public static void writeIntMessage(Pipe ring, boolean isNullable, boolean isSigned, String columnName, Object value) {
        // int is written as int32/uint32
        writeNullableIntMessage(ring, isNullable, isSigned, columnName, value);
        if (value != null)
            writeIntMessage(ring, isNullable, isSigned, columnName, ((Number)value).intValue());
    }

    public static void writeBooleanMessage(Pipe ring, boolean isNullable, String columnName, Object value) {
        writeNullableMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_NULLABLEBOOLEAN_LOC : MetaMessageDefs.MSG_NAMEDNULLABLEBOOLEAN_LOC, isNullable, columnName, value);
        if (value != null) {
        	Pipe.blockWriteMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_BOOLEAN_LOC : MetaMessageDefs.MSG_NAMEDBOOLEAN_LOC);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
            Pipe.addIntValue(((Boolean) value).booleanValue() ? 1 : 0, ring);
            Pipe.publishWrites(ring);
        }
    }


    public static void writeLongMessage(Pipe ring, boolean isNullable, boolean isSigned, String columnName, Object value) {
        // long is written as int64/uint64
        writeNullableMessage(ring, (!isSigned && (columnName == null)) ? MetaMessageDefs.MSG_NULLABLEUINT64_LOC : (isSigned && (columnName == null)) ? MetaMessageDefs.MSG_NULLABLEINT64_LOC
                : (!isSigned && (columnName != null)) ? MetaMessageDefs.MSG_NAMEDNULLABLEUINT64_LOC : MetaMessageDefs.MSG_NAMEDNULLABLEINT64_LOC, isNullable, columnName, value);
        if (value != null) {
            int msg = (!isSigned && (columnName == null)) ? MetaMessageDefs.MSG_UINT64_LOC : (isSigned && (columnName == null)) ? MetaMessageDefs.MSG_INT64_LOC : (!isSigned && (columnName != null)) ? MetaMessageDefs.MSG_NAMEDUINT64_LOC
                    : MetaMessageDefs.MSG_NAMEDINT64_LOC;
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
            Pipe.addLongValue(((Number)value).longValue(), ring);
            Pipe.publishWrites(ring);
        }
    }

    public static void writeDecimalMessage(Pipe ring, boolean isNullable, String columnName, Object value) {
        // long is written as int64/uint64
        writeNullableMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_NULLABLEDECIMAL_LOC : MetaMessageDefs.MSG_NAMEDNULLABLEDECIMAL_LOC, isNullable, columnName, value);

        if (value != null) {
            int msg = (columnName == null) ? MetaMessageDefs.MSG_DECIMAL_LOC : MetaMessageDefs.MSG_NAMEDDECIMAL_LOC;
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
            // separate mantissa and exponent
            BigDecimal bd = (BigDecimal) value;
            Pipe.addDecimal(bd.scale(), bd.unscaledValue().longValue(), ring);
            Pipe.publishWrites(ring);
        }
    }


    public static void writeDoubleMessage(Pipe ring, boolean isNullable, String columnName, Object value) {
        // double is written as int64
        writeNullableMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_NULLABLEDOUBLE_LOC : MetaMessageDefs.MSG_NAMEDNULLABLEDOUBLE_LOC, isNullable, columnName, value);
        if (value != null) {
            int msg = (columnName == null) ? MetaMessageDefs.MSG_DOUBLE_LOC : MetaMessageDefs.MSG_NAMEDDOUBLE_LOC;
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
            long bits = Double.doubleToLongBits( ((Number)value).doubleValue());
            Pipe.addLongValue(bits, ring);
            Pipe.publishWrites(ring);
        }
    }


    public static void writeFloatMessage(Pipe ring, boolean isNullable, String columnName, Object value) {
        // double is written as int64
        writeNullableMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_NULLABLEFLOAT_LOC : MetaMessageDefs.MSG_NAMEDNULLABLEFLOAT_LOC, isNullable, columnName, value);    
        if (value != null) {
            int msg = (columnName == null) ? MetaMessageDefs.MSG_FLOAT_LOC : MetaMessageDefs.MSG_NAMEDFLOAT_LOC;
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
            int bits = Float.floatToIntBits( ((Number)value).floatValue());
            Pipe.addIntValue(bits, ring);
            Pipe.publishWrites(ring);
        }
    }



    public static void writeDateTimeMessage(Pipe ring, boolean isNullable, String columnName, Object value) {
        // datetime is written as int64
        writeNullableMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_NULLABLEDATETIME_LOC : MetaMessageDefs.MSG_NAMEDNULLABLEDATETIME_LOC, isNullable, columnName, value);
        if (value != null) {
            int msg = (columnName == null) ? MetaMessageDefs.MSG_DATETIME_LOC : MetaMessageDefs.MSG_NAMEDDATETIME_LOC;
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
            long millisecondsSinceEpoch = ((java.util.Date) value).getTime(); // java.sql.Date, java.sql.Time,
                                                                              // java.sql.Timestamp
            Pipe.addLongValue(millisecondsSinceEpoch, ring);
            Pipe.publishWrites(ring);
        }
    }
    
    public static void writeTimestampMessage(Pipe ring, boolean isNullable, String columnName, java.sql.Timestamp timestamp, int tzOffset) {
        writeNullableMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_NULLABLETIMESTAMP_LOC : MetaMessageDefs.MSG_NAMEDNULLABLETIMESTAMP_LOC, isNullable, columnName, timestamp);
        if (timestamp != null) {
            int msg = (columnName == null) ? MetaMessageDefs.MSG_TIMESTAMP_LOC : MetaMessageDefs.MSG_NAMEDTIMESTAMP_LOC;
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
            long millisecondsSinceEpoch = timestamp.getTime();
            Pipe.addLongValue(millisecondsSinceEpoch, ring);
            Pipe.addIntValue(timestamp.getNanos(), ring);
            Pipe.addIntValue(tzOffset, ring);
            Pipe.publishWrites(ring);
        }
    }

    public static void writeByteArrayMessage(Pipe ring, boolean isNullable, String columnName, Object value) {
        writeNullableMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_NULLABLEBYTEARRAY_LOC : MetaMessageDefs.MSG_NAMEDNULLABLEBYTEARRAY_LOC, isNullable, columnName, value);
        if (value != null) {
            int msg = (columnName == null) ? MetaMessageDefs.MSG_BYTEARRAY_LOC : MetaMessageDefs.MSG_NAMEDBYTEARRAY_LOC;
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
			byte[] source = (byte[]) value;
            Pipe.addByteArray(source, 0, source.length, ring);
            Pipe.publishWrites(ring);
        }
    }

    public static void writeSerializedMessage(Pipe ring, boolean isNullable, String columnName, Object value) throws IOException {
        writeNullableMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_NULLABLESERIALIZEDJAVAOBJECT_LOC : MetaMessageDefs.MSG_NAMEDNULLABLESERIALIZEDJAVAOBJECT_LOC, isNullable, columnName, value);
        if (value != null) {
            int msg = (columnName == null) ? MetaMessageDefs.MSG_SERIALIZEDJAVAOBJECT_LOC : MetaMessageDefs.MSG_NAMEDSERIALIZEDJAVAOBJECT_LOC;
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = null;
            byte[] bytes = null;
            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(value);
                bytes = bos.toByteArray();
            } finally {
                try {
                    if (out != null)
                        out.close();
                } catch (IOException ex) {
                    // ignore close exception
                }
                try {
                    bos.close();
                } catch (IOException ex) {
                    // ignore close exception
                }
            }

            if (bytes == null)
                throw new InvalidObjectException("Could not serialize " + value);

            Pipe.addByteArray(bytes, 0, bytes.length, ring);
            Pipe.publishWrites(ring);
        }
    }

    public static void writeBeginGroupMessage(Pipe ring, String name) {
        int msg = (name == null) ? MetaMessageDefs.MSG_BEGINGROUP_LOC : MetaMessageDefs.MSG_NAMEDBEGINGROUP_LOC;
        Pipe.blockWriteMessage(ring, msg);
        if (name != null) {
            Pipe.validateVarLength(ring, name.length());
			int sourceLen = name.length();
			final int p = Pipe.copyASCIIToBytes(name, 0, sourceLen, ring); 
			Pipe.addBytePosAndLen(ring, p, sourceLen);
        }
        Pipe.publishWrites(ring);
    }

    public static void writeEndGroupMessage(Pipe ring) {
    	Pipe.blockWriteMessage(ring, MetaMessageDefs.MSG_ENDGROUP_LOC);
        Pipe.publishWrites(ring);
    }

    public static void writeASCIIMessage(Pipe ring, boolean isNullable, String columnName, Object value) {
        writeNullableMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_NULLABLEASCII_LOC : MetaMessageDefs.MSG_NAMEDNULLABLEASCII_LOC, isNullable, columnName, value);
        if (value != null) {
            int msg = (columnName == null) ? MetaMessageDefs.MSG_ASCII_LOC : MetaMessageDefs.MSG_NAMEDASCII_LOC;
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
			CharSequence source = (String) value;
            Pipe.validateVarLength(ring, source.length());
			int sourceLen = source.length();
			final int p = Pipe.copyASCIIToBytes(source, 0, sourceLen, ring); 
			Pipe.addBytePosAndLen(ring, p, sourceLen);
            Pipe.publishWrites(ring);
        }
    }
    
    public static void writeUTF8Message(Pipe ring, boolean isNullable, String columnName, Object value) {
        writeNullableMessage(ring, (columnName == null) ? MetaMessageDefs.MSG_NULLABLEUTF8_LOC : MetaMessageDefs.MSG_NAMEDNULLABLEUTF8_LOC, isNullable, columnName, value);
        if (value != null) {
            int msg = (columnName == null) ? MetaMessageDefs.MSG_UTF8_LOC : MetaMessageDefs.MSG_NAMEDUTF8_LOC;
            Pipe.blockWriteMessage(ring, msg);
            if (columnName != null) {
                Pipe.validateVarLength(ring, columnName.length());
				int sourceLen = columnName.length();
				final int p = Pipe.copyASCIIToBytes(columnName, 0, sourceLen, ring); 
				Pipe.addBytePosAndLen(ring, p, sourceLen);
            }
			CharSequence source = (String) value;
            Pipe.validateVarLength(ring, source.length()<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
			Pipe.addBytePosAndLen(ring, Pipe.getBlobWorkingHeadPosition( ring), Pipe.copyUTF8ToByte(source, source.length(), ring));
            Pipe.publishWrites(ring);
        }
    }

}
