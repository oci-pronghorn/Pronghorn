package com.ociweb.pronghorn.columns;

public class TypeDef<
        L extends Enum<L> & FieldsOf64Bits,
        I extends Enum<I> & FieldsOf32Bits,
        S extends Enum<S> & FieldsOf16Bits,
        B extends Enum<B> & FieldsOf8Bits> {

    public final L[] longFields;
    public final I[] intFields;
    public final S[] shortFields;
    public final B[] byteFields;
    
    public final int longFieldCount;
    public final int intFieldCount;
    public final int shortFieldCount;
    public final int byteFieldCount;
    
    public TypeDef(Class<L> longFields, Class<I> intFields, Class<S> shortFields, Class<B> byteFields) {
        this.longFields= longFields.getEnumConstants();
        this.intFields = intFields.getEnumConstants();
        this.shortFields = shortFields.getEnumConstants();
        this.byteFields = byteFields.getEnumConstants();
        
        this.longFieldCount = this.longFields.length;
        this.intFieldCount = this.intFields.length;
        this.shortFieldCount = this.shortFields.length;
        this.byteFieldCount = this.byteFields.length;
        
    }
    
}
