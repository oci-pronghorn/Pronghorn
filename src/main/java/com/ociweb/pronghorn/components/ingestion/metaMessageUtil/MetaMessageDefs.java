package com.ociweb.pronghorn.components.ingestion.metaMessageUtil;

import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupTemplateLocator;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;


public class MetaMessageDefs  {

    public static final FieldReferenceOffsetManager FROM = MetaMessageUtil.buildFROM("/metaTemplate.xml");

    public static final int MSG_NULL_LOC = lookupTemplateLocator("Null",MetaMessageDefs.FROM);
    public static final int MSG_MESSAGE_BEGIN_LOC = lookupTemplateLocator("BeginMessage",MetaMessageDefs.FROM);  
    public static final int MSG_MESSAGE_END_LOC = lookupTemplateLocator("EndMessage",MetaMessageDefs.FROM);  
    public static final int MSG_FLUSH = lookupTemplateLocator("Flush",MetaMessageDefs.FROM); 
    
    // public so consumers can share the definitions
    // uInt32
    public static final int MSG_UINT32_LOC = lookupTemplateLocator("UInt32", FROM);
    public static final int UINT32_VALUE_LOC = lookupFieldLocator("Value", MSG_UINT32_LOC, FROM);
    public static final int MSG_NAMEDUINT32_LOC = lookupTemplateLocator("NamedUInt32", FROM);
    public static final int NAMEDUINT32_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDUINT32_LOC, FROM);
    public static final int NAMEDUINT32_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDUINT32_LOC, FROM);
    public static final int MSG_NULLABLEUINT32_LOC = lookupTemplateLocator("NullableUInt32", FROM);
    public static final int NULLABLEUINT32_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEUINT32_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEUINT32_LOC = lookupTemplateLocator("NamedNullableUInt32", FROM);
    public static final int NAMEDNULLABLEUINT32_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEUINT32_LOC, FROM);
    public static final int NAMEDNULLABLEUINT32_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEUINT32_LOC, FROM);
    
    // Int32
    public static final int MSG_INT32_LOC = lookupTemplateLocator("Int32", FROM);
    public static final int INT32_VALUE_LOC = lookupFieldLocator("Value", MSG_INT32_LOC, FROM);
    public static final int MSG_NAMEDINT32_LOC = lookupTemplateLocator("NamedInt32", FROM);
    
    public static final int NAMEDINT32_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDINT32_LOC, FROM);
    public static final int NAMEDINT32_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDINT32_LOC, FROM);
    
    public static final int MSG_NULLABLEINT32_LOC = lookupTemplateLocator("NullableInt32", FROM);
    public static final int NULLABLEINT32_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEINT32_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEINT32_LOC = lookupTemplateLocator("NamedNullableInt32", FROM);
    public static final int NAMEDNULLABLEINT32_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEINT32_LOC, FROM);
    public static final int NAMEDNULLABLEINT32_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEINT32_LOC, FROM);

    // uInt64
    public static final int MSG_UINT64_LOC = lookupTemplateLocator("UInt64", FROM);
    public static final int MSG_NAMEDUINT64_LOC = lookupTemplateLocator("NamedUInt64", FROM);
    public static final int MSG_NULLABLEUINT64_LOC = lookupTemplateLocator("NullableUInt64", FROM);
    public static final int MSG_NAMEDNULLABLEUINT64_LOC = lookupTemplateLocator("NamedNullableUInt64", FROM);
    public static final int NAMEDUINT64_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDUINT64_LOC, FROM);
    public static final int NAMEDUINT64_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDUINT64_LOC, FROM);
    public static final int UINT64_VALUE_LOC = lookupFieldLocator("Value", MSG_UINT64_LOC, FROM);
    
    
    // int64
    public static final int MSG_INT64_LOC = lookupTemplateLocator("Int64", FROM);
    public static final int INT64_VALUE_LOC = lookupFieldLocator("Value", MSG_INT64_LOC, FROM);
    public static final int MSG_NAMEDINT64_LOC = lookupTemplateLocator("NamedInt64", FROM);
    public static final int NAMEDINT64_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDINT64_LOC, FROM);
    public static final int NAMEDINT64_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDINT64_LOC, FROM);
    public static final int MSG_NULLABLEINT64_LOC = lookupTemplateLocator("NullableInt64", FROM);
    public static final int NULLABLEINT64_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEINT64_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEINT64_LOC = lookupTemplateLocator("NamedNullableInt64", FROM);
    public static final int NAMEDNULLABLEINT64_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEINT64_LOC, FROM);
    public static final int NAMEDNULLABLEINT64_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEINT64_LOC, FROM);

    // ASCII
    public static final int MSG_ASCII_LOC = lookupTemplateLocator("ASCII", FROM);
    public static final int ASCII_VALUE_LOC = lookupFieldLocator("Value", MSG_ASCII_LOC, FROM);
    public static final int MSG_NAMEDASCII_LOC = lookupTemplateLocator("NamedASCII", FROM);
    public static final int NAMEDASCII_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDASCII_LOC, FROM);
    public static final int NAMEDASCII_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDASCII_LOC, FROM);
    public static final int MSG_NULLABLEASCII_LOC = lookupTemplateLocator("NullableASCII", FROM);
    public static final int NULLABLEASCII_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEASCII_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEASCII_LOC = lookupTemplateLocator("NamedNullableASCII", FROM);
    public static final int NAMEDNULLABLEASCII_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEASCII_LOC, FROM);
    public static final int NAMEDNULLABLEASCII_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEASCII_LOC, FROM);
    
    // UTF8
    public static final int MSG_UTF8_LOC = lookupTemplateLocator("UTF8", FROM);
    public static final int UTF8_VALUE_LOC = lookupFieldLocator("Value", MSG_UTF8_LOC, FROM);
    public static final int MSG_NAMEDUTF8_LOC = lookupTemplateLocator("NamedUTF8", FROM);
    public static final int NAMEDUTF8_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDUTF8_LOC, FROM);
    public static final int NAMEDUTF8_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDUTF8_LOC, FROM);
    public static final int MSG_NULLABLEUTF8_LOC = lookupTemplateLocator("NullableUTF8", FROM);
    public static final int NULLABLEUTF8_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEUTF8_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEUTF8_LOC = lookupTemplateLocator("NamedNullableUTF8", FROM);
    public static final int NAMEDNULLABLEUTF8_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEUTF8_LOC, FROM);
    public static final int NAMEDNULLABLEUTF8_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEUTF8_LOC, FROM);

    // decimal
    public static final int MSG_DECIMAL_LOC = lookupTemplateLocator("Decimal", FROM);
    public static final int DECIMAL_VALUE_LOC = lookupFieldLocator("Value", MSG_DECIMAL_LOC, FROM);
    public static final int MSG_NAMEDDECIMAL_LOC = lookupTemplateLocator("NamedDecimal", FROM);
    public static final int NAMEDDECIMAL_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDDECIMAL_LOC, FROM);
    public static final int NAMEDDECIMAL_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDDECIMAL_LOC, FROM);
    public static final int MSG_NULLABLEDECIMAL_LOC = lookupTemplateLocator("NullableDecimal", FROM);
    public static final int NULLABLEDECIMAL_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEDECIMAL_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEDECIMAL_LOC = lookupTemplateLocator("NamedNullableDecimal", FROM);
    public static final int NAMEDNULLABLEDECIMAL_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEDECIMAL_LOC, FROM);
    public static final int NAMEDNULLABLEDECIMAL_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEDECIMAL_LOC, FROM);

    // byteArray
    public static final int MSG_BYTEARRAY_LOC = lookupTemplateLocator("ByteArray", FROM);
    public static final int BYTEARRAY_VALUE_LOC = lookupFieldLocator("Value", MSG_BYTEARRAY_LOC, FROM);
    public static final int MSG_NAMEDBYTEARRAY_LOC = lookupTemplateLocator("NamedByteArray", FROM);
    public static final int NAMEDBYTEARRAY_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDBYTEARRAY_LOC, FROM);
    public static final int NAMEDBYTEARRAY_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDBYTEARRAY_LOC, FROM);
    public static final int MSG_NULLABLEBYTEARRAY_LOC = lookupTemplateLocator("NullableByteArray", FROM);
    public static final int NULLABLEBYTEARRAY_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEBYTEARRAY_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEBYTEARRAY_LOC = lookupTemplateLocator("NamedNullableByteArray", FROM);
    public static final int NAMEDNULLABLEBYTEARRAY_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEBYTEARRAY_LOC, FROM);
    public static final int NAMEDNULLABLEBYTEARRAY_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEBYTEARRAY_LOC, FROM);

    // group
    public static final int MSG_BEGINGROUP_LOC = lookupTemplateLocator("BeginGroup", FROM);
    public static final int MSG_NAMEDBEGINGROUP_LOC = lookupTemplateLocator("NamedBeginGroup", FROM);
    public static final int NAMEDBEGINGROUP_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDBEGINGROUP_LOC, FROM);
    public static final int MSG_ENDGROUP_LOC = lookupTemplateLocator("EndGroup", FROM);

    // boolean
    public static final int MSG_BOOLEAN_LOC = lookupTemplateLocator("Boolean", FROM);
    public static final int BOOLEAN_VALUE_LOC = lookupFieldLocator("Value", MSG_BOOLEAN_LOC, FROM);
    public static final int MSG_NAMEDBOOLEAN_LOC = lookupTemplateLocator("NamedBoolean", FROM);
    public static final int NAMEDBOOLEAN_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDBOOLEAN_LOC, FROM);
    public static final int NAMEDBOOLEAN_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDBOOLEAN_LOC, FROM);
    public static final int MSG_NULLABLEBOOLEAN_LOC = lookupTemplateLocator("NullableBoolean", FROM);
    public static final int NULLABLEBOOLEAN_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEBOOLEAN_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEBOOLEAN_LOC = lookupTemplateLocator("NamedNullableBoolean", FROM);
    public static final int NAMEDNULLABLEBOOLEAN_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEBOOLEAN_LOC, FROM);
    public static final int NAMEDNULLABLEBOOLEAN_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEBOOLEAN_LOC, FROM);

    // float
    public static final int MSG_FLOAT_LOC = lookupTemplateLocator("Float", FROM);
    public static final int FLOAT_VALUE_LOC = lookupFieldLocator("Value", MSG_FLOAT_LOC, FROM);
    public static final int MSG_NAMEDFLOAT_LOC = lookupTemplateLocator("NamedFloat", FROM);
    public static final int NAMEDFLOAT_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDFLOAT_LOC, FROM);
    public static final int NAMEDFLOAT_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDFLOAT_LOC, FROM);
    public static final int MSG_NULLABLEFLOAT_LOC = lookupTemplateLocator("NullableFloat", FROM);
    public static final int NULLABLEFLOAT_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEFLOAT_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEFLOAT_LOC = lookupTemplateLocator("NamedNullableFloat", FROM);
    public static final int NAMEDNULLABLEFLOAT_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEFLOAT_LOC, FROM);
    public static final int NAMEDNULLABLEFLOAT_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEFLOAT_LOC, FROM);

    // double
    public static final int MSG_DOUBLE_LOC = lookupTemplateLocator("Double", FROM);
    public static final int DOUBLE_VALUE_LOC = lookupFieldLocator("Value", MSG_DOUBLE_LOC, FROM);
    public static final int MSG_NAMEDDOUBLE_LOC = lookupTemplateLocator("NamedDouble", FROM);
    public static final int NAMEDDOUBLE_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDDOUBLE_LOC, FROM);
    public static final int NAMEDDOUBLE_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDDOUBLE_LOC, FROM);
    public static final int MSG_NULLABLEDOUBLE_LOC = lookupTemplateLocator("NullableDouble", FROM);
    public static final int NULLABLEDOUBLE_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEDOUBLE_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEDOUBLE_LOC = lookupTemplateLocator("NamedNullableDouble", FROM);
    public static final int NAMEDNULLABLEDOUBLE_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEDOUBLE_LOC, FROM);
    public static final int NAMEDNULLABLEDOUBLE_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEDOUBLE_LOC, FROM);

    // date time
    public static final int MSG_DATETIME_LOC = lookupTemplateLocator("DateTime", FROM);
    public static final int DATETIME_VALUE_LOC = lookupFieldLocator("Value", MSG_DATETIME_LOC, FROM);
    public static final int MSG_NAMEDDATETIME_LOC = lookupTemplateLocator("NamedDateTime", FROM);
    public static final int NAMEDDATETIME_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDDATETIME_LOC, FROM);
    public static final int NAMEDDATETIME_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDDATETIME_LOC, FROM);
    public static final int MSG_NULLABLEDATETIME_LOC = lookupTemplateLocator("NullableDateTime", FROM);
    public static final int NULLABLEDATETIME_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLEDATETIME_LOC, FROM);
    public static final int MSG_NAMEDNULLABLEDATETIME_LOC = lookupTemplateLocator("NamedNullableDateTime", FROM);
    public static final int NAMEDNULLABLEDATETIME_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLEDATETIME_LOC, FROM);
    public static final int NAMEDNULLABLEDATETIME_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLEDATETIME_LOC, FROM);

    // serialized Java object
    public static final int MSG_SERIALIZEDJAVAOBJECT_LOC = lookupTemplateLocator("SerializedJavaObject", FROM);
    public static final int SERIALIZEDJAVAOBJECT_VALUE_LOC = lookupFieldLocator("Value", MSG_SERIALIZEDJAVAOBJECT_LOC, FROM);
    public static final int MSG_NAMEDSERIALIZEDJAVAOBJECT_LOC = lookupTemplateLocator("NamedSerializedJavaObject", FROM);
    public static final int NAMEDSERIALIZEDJAVAOBJECT_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDSERIALIZEDJAVAOBJECT_LOC, FROM);
    public static final int NAMEDSERIALIZEDJAVAOBJECT_VALUE_LOC = lookupFieldLocator("Value", MSG_NAMEDSERIALIZEDJAVAOBJECT_LOC, FROM);
    public static final int MSG_NULLABLESERIALIZEDJAVAOBJECT_LOC = lookupTemplateLocator("NullableSerializedJavaObject", FROM);
    public static final int NULLABLESERIALIZEDJAVAOBJECT_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLESERIALIZEDJAVAOBJECT_LOC, FROM);
    public static final int MSG_NAMEDNULLABLESERIALIZEDJAVAOBJECT_LOC = lookupTemplateLocator("NamedNullableSerializedJavaObject", FROM);
    public static final int NAMEDNULLABLESERIALIZEDJAVAOBJECT_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLESERIALIZEDJAVAOBJECT_LOC, FROM);
    public static final int NAMEDNULLABLESERIALIZEDJAVAOBJECT_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLESERIALIZEDJAVAOBJECT_LOC, FROM);
    
    // timestamp
    public static final int MSG_TIMESTAMP_LOC = lookupTemplateLocator("Timestamp", FROM);
    public static final int TIMESTAMP_DATETIME_LOC = lookupFieldLocator("DateTime", MSG_TIMESTAMP_LOC, FROM);
    public static final int TIMESTAMP_NANOS_LOC = lookupFieldLocator("Nanos", MSG_TIMESTAMP_LOC, FROM);
    public static final int TIMESTAMP_TZOFFSET_LOC = lookupFieldLocator("TZOffset", MSG_TIMESTAMP_LOC, FROM);
    public static final int MSG_NAMEDTIMESTAMP_LOC = lookupTemplateLocator("NamedTimestamp", FROM);
    public static final int NAMEDTIMESTAMP_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDTIMESTAMP_LOC, FROM);
    public static final int NAMEDTIMESTAMP_DATETIME_LOC = lookupFieldLocator("DateTime", MSG_NAMEDTIMESTAMP_LOC, FROM);
    public static final int NAMEDTIMESTAMP_NANOS_LOC = lookupFieldLocator("Nanos", MSG_NAMEDTIMESTAMP_LOC, FROM);
    public static final int NAMEDTIMESTAMP_TZOFFSET_LOC = lookupFieldLocator("TZOffset", MSG_NAMEDTIMESTAMP_LOC, FROM);
    public static final int MSG_NULLABLETIMESTAMP_LOC = lookupTemplateLocator("NullableTimestamp", FROM);
    public static final int NULLABLETIMESTAMP_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NULLABLETIMESTAMP_LOC, FROM);
    public static final int MSG_NAMEDNULLABLETIMESTAMP_LOC = lookupTemplateLocator("NamedNullableTimestamp", FROM);
    public static final int NAMEDNULLABLETIMESTAMP_NAME_LOC = lookupFieldLocator("Name", MSG_NAMEDNULLABLETIMESTAMP_LOC, FROM);
    public static final int NAMEDNULLABLETIMESTAMP_NOTNULL_LOC = lookupFieldLocator("NotNull", MSG_NAMEDNULLABLETIMESTAMP_LOC, FROM);
    
    
	public static final int MSG_NAMED_NULL_LOC = lookupTemplateLocator("NamedNull", FROM);  
	public static final int NAMED_NULL_NAME_LOC = lookupFieldLocator("Name", MSG_NAMED_NULL_LOC,  FROM);
    
    
}
