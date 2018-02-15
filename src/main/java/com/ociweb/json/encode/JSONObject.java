package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

import java.util.function.ToLongFunction;

public class JSONObject<T, P extends JSONComplete> implements JSONComplete  {
    private final JSONBuilder<T> builder;
    private final P owner;
    private final int depth;

    JSONObject(StringTemplateBuilder<T> scripts, JSONKeywords keywords, P owner, int depth) {
        this.depth = depth;
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
        this.owner = owner;
    }

    @Override
    public void complete() {
    }

    public JSONObject<T, JSONObject<T, P>> beginObject(String name) {
        return new JSONObject<>(
                builder.addFieldPrefix(name).beginObject(),
                builder.getKeywords(), this, depth + 1);
    }

    public JSONObject<T, JSONObject<T, P>> beginNullableObject(String name, ToBoolFunction<T> isNull) {
        return new JSONObject<>(
                builder.addFieldPrefix(name).beginObject(isNull),
                builder.getKeywords(), this, depth + 1);
    }

    public <N> JSONArray<T, JSONObject<T, P>, N> array(String name, ArrayIteratorFunction<T, N> iterator) {
        return new JSONArray<>(
                builder.addFieldPrefix(name).beginArray(),
                builder.getKeywords(), iterator, this, depth + 1);
    }

    public <N> JSONArray<T, JSONObject<T, P>, N> nullableArray(String name, ToBoolFunction<T> isNull, ArrayIteratorFunction<T, N> iterator) {
        return new JSONArray<>(
                builder.addFieldPrefix(name).beginArray(isNull),
                builder.getKeywords(), iterator, this, depth + 1);
    }

    public JSONObject<T, P> constantNull(String name) {
        builder.addFieldPrefix(name).addNull();
        return this;
    }

    public JSONObject<T, P> bool(String name, ToBoolFunction<T> func) {
        builder.addFieldPrefix(name).addBool(func);
        return this;
    }

    public JSONObject<T, P> bool(String name, ToBoolFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addBool(func, encode);
        return this;
    }

    public JSONObject<T, P> nullableBool(String name, ToNullableBoolFunction<T> func) {
        builder.addFieldPrefix(name).addBool(func);
        return this;
    }

    public JSONObject<T, P> nullableBool(String name, ToNullableBoolFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addBool(func, encode);
        return this;
    }

    public JSONObject<T, P> integer(String name, ToLongFunction<T> func) {
        builder.addFieldPrefix(name).addInteger(func);
        return this;
    }

    public JSONObject<T, P> integer(String name, ToLongFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addInteger(func, encode);
        return this;
    }

    public JSONObject<T, P> nullableInteger(String name, ToNullableLongFunction<T> func) {
        builder.addFieldPrefix(name).addInteger(func);
        return this;
    }

    public JSONObject<T, P> nullableInteger(String name, ToNullableLongFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addInteger(func, encode);
        return this;
    }

    public JSONObject<T, P> decimal(String name, ToDecimalFunction<T> func) {
        builder.addFieldPrefix(name).addDecimal(func);
        return this;
    }

    public JSONObject<T, P> decimal(String name, ToDecimalFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addDecimal(func, encode);
        return this;
    }

    public JSONObject<T, P> nullableDecimal(String name, ToNullableDecimalFunction<T> func) {
        builder.addFieldPrefix(name).addDecimal(func);
        return this;
    }

    public JSONObject<T, P> nullableDecimal(String name, ToNullableDecimalFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addDecimal(func, encode);
        return this;
    }

    public JSONObject<T, P> string(String name, ToStringFunction<T> func) {
        builder.addFieldPrefix(name).addString(func);
        return this;
    }

    public JSONObject<T, P> string(String name, ToStringFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addString(func, encode);
        return this;
    }

    public JSONObject<T, P> nullableString(String name, ToStringFunction<T> func) {
        builder.addFieldPrefix(name).addNullableString(func);
        return this;
    }

    public JSONObject<T, P> nullableString(String name, ToStringFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addNullableString(func, encode);
        return this;
    }

    public P endObject() {
        builder.endObject();
        owner.complete();
        return owner;
    }
}
