package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

import java.util.function.ToLongFunction;

// TODO: fix complete not laways getting called

public class JSONRoot<T, P extends JSONRoot> implements JSONComplete {
    final JSONBuilder<T> builder;
    private final P owner;
    private final int depth;

    JSONRoot(StringTemplateBuilder<T> scripts, P owner, JSONKeywords keywords, int depth) {
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
        this.owner = owner;
        this.depth = depth;
        builder.start();
    }

    JSONRoot(StringTemplateBuilder<T> scripts, JSONKeywords keywords, int depth) {
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
        this.owner = (P)this;
        this.depth = depth;
        builder.start();
    }

    @Override
    public void complete() {
        builder.complete();
    }

    public JSONObject<T, P> beginObject() {
        return new JSONObject<>(
                builder.beginObject(),
                builder.getKeywords(), owner, depth + 1);
    }

    public JSONObject<T, P> beginNullObject(ToBoolFunction<T> isNull) {
        return new JSONObject<>(
                builder.beginObject(isNull),
                builder.getKeywords(), owner, depth + 1);
    }

    public <N> JSONArray<T, P, N> array(ArrayIteratorFunction<T, N> iterator) {
        return new JSONArray<>(
                builder.beginArray(),
                builder.getKeywords(), iterator, owner, depth + 1);
    }

    public <N> JSONArray<T, P, N> nullableArray(ToBoolFunction<T> isNull, ArrayIteratorFunction<T, N> iterator) {
        return new JSONArray<>(
                builder.beginArray(isNull),
                builder.getKeywords(), iterator, owner, depth + 1);
    }

    public P empty() {
        this.complete();
        return owner;
    }

    public P constantNull() {
        builder.addNull();
        this.complete();
        return owner;
    }

    public P bool(ToBoolFunction<T> func) {
        builder.addBool(func);
        this.complete();
        return owner;
    }

    public P bool(ToBoolFunction<T> func, JSONType encode) {
        builder.addBool(func, encode);
        this.complete();
        return owner;
    }

    public P nullableBool(ToNullableBoolFunction<T> func) {
        builder.addBool(func);
        this.complete();
        return owner;
    }

    public P nullableBool(ToNullableBoolFunction<T> func, JSONType encode) {
        builder.addBool(func, encode);
        this.complete();
        return owner;
    }

    public P integer(ToLongFunction<T> func) {
        builder.addInteger(func);
        this.complete();
        return owner;
    }

    public P integer(ToLongFunction<T> func, JSONType encode) {
        builder.addInteger(func, encode);
        this.complete();
        return owner;
    }

    public P nullableInteger(ToNullableLongFunction<T> func) {
        builder.addInteger(func);
        this.complete();
        return owner;
    }

    public P nullableInteger(ToNullableLongFunction<T> func, JSONType encode) {
        builder.addInteger(func, encode);
        this.complete();
        return owner;
    }

    public P decimal(ToDecimalFunction<T> func) {
        builder.addDecimal(func);
        this.complete();
        return owner;
    }

    public P decimal(ToDecimalFunction<T> func, JSONType encode) {
        builder.addDecimal(func, encode);
        this.complete();
        return owner;
    }

    public P nullableDecimal(ToNullableDecimalFunction<T> func) {
        builder.addDecimal(func);
        return owner;
    }

    public P nullableDecimal(ToNullableDecimalFunction<T> func, JSONType encode) {
        builder.addDecimal(func, encode);
        this.complete();
        return owner;
    }

    public P string(ToStringFunction<T> func) {
        builder.addString(func);
        this.complete();
        return owner;
    }

    public P string(ToStringFunction<T> func, JSONType encode) {
        builder.addString(func, encode);
        this.complete();
        return owner;
    }

    public P nullableString(ToStringFunction<T> func) {
        builder.addString(func);
        this.complete();
        return owner;
    }

    public P nullableString(ToStringFunction<T> func, JSONType encode) {
        builder.addString(func, encode);
        this.complete();
        return owner;
    }
}
