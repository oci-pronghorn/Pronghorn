package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.encode.template.StringTemplateBuilder;

import java.util.function.ToIntFunction;

public class JSONArray<T, P extends JSONComplete> {
    private final JSONBuilder<T> builder;
    private ToIntFunction<T> arrayLength;
    private final P owner;
    private final int depth;

    JSONArray(StringTemplateBuilder<T> scripts, JSONKeywords keywords, ToIntFunction<T> arrayLength, P owner, int depth) {
        this.arrayLength = arrayLength;
        this.depth = depth;
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
        this.builder.setArrayLength(arrayLength);
        this.owner = owner;
    }

    // TODO: all other element types
    // TODO create an Array hosted version of JSONArray and JSONObject to pass index int[]

    public P constantNull() {
        builder.addNull();
        owner.complete();
        return owner;
    }

    public P integer(IterLongFunction<T> func) {
        builder.addInteger(func);
        owner.complete();
        return owner;
    }

    public P integer(IterLongFunction<T> func, JSONType encode) {
        builder.addInteger(func, encode);
        owner.complete();
        return owner;
    }

    public P integerNull(IterNullableLongFunction<T> func) {
        builder.addInteger(func);
        owner.complete();
        return owner;
    }

    public P integerNull(IterNullableLongFunction<T> func, JSONType encode) {
        builder.addInteger(func, encode);
        owner.complete();
        return owner;
    }
}
