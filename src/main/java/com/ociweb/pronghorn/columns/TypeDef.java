package com.ociweb.pronghorn.columns;

public class TypeDef<
        L extends Enum<L> & FieldsOf64Bits,
        I extends Enum<I> & FieldsOf32Bits,
        S extends Enum<S> & FieldsOf16Bits,
        B extends Enum<B> & FieldsOf8Bits> {

    public final Class<L> longFields;
    public final Class<I> intFields;
    public final Class<S> shortFields;
    public final Class<B> byteFields;
    
    public TypeDef(Class<L> longFields, Class<I> intFields, Class<S> shortFields, Class<B> byteFields) {
        this.longFields=longFields;
        this.intFields = intFields;
        this.shortFields = shortFields;
        this.byteFields = byteFields;
    }
    
}
