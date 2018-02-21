package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

// TODO: fix complete not aways getting called

public class JSONRoot<T, P extends JSONRoot> implements JSONCompositeOwner {
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
    public void childCompleted() {
        // Single child...
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
        this.childCompleted();
        return owner;
    }

    public P constantNull() {
        builder.addNull();
        this.childCompleted();
        return owner;
    }

    public P bool(ToBoolFunction<T> func) {
        builder.addBool(func);
        this.childCompleted();
        return owner;
    }

    public P bool(ToBoolFunction<T> func, JSONType encode) {
        builder.addBool(func, encode);
        this.childCompleted();
        return owner;
    }

    public P nullableBool(ToNullableBoolFunction<T> func) {
        builder.addBool(func);
        this.childCompleted();
        return owner;
    }

    public P nullableBool(ToNullableBoolFunction<T> func, JSONType encode) {
        builder.addBool(func, encode);
        this.childCompleted();
        return owner;
    }

    public P integer(ToLongFunction<T> func) {
        builder.addInteger(func);
        this.childCompleted();
        return owner;
    }

    public P integer(ToLongFunction<T> func, JSONType encode) {
        builder.addInteger(func, encode);
        this.childCompleted();
        return owner;
    }

    public P nullableInteger(ToNullableLongFunction<T> func) {
        builder.addInteger(func);
        this.childCompleted();
        return owner;
    }

    public P nullableInteger(ToNullableLongFunction<T> func, JSONType encode) {
        builder.addInteger(func, encode);
        this.childCompleted();
        return owner;
    }

    public P decimal(ToDoubleFunction<T> func) {
        builder.addDecimal(func);
        this.childCompleted();
        return owner;
    }

    public P decimal(ToDoubleFunction<T> func, JSONType encode) {
        builder.addDecimal(func, encode);
        this.childCompleted();
        return owner;
    }

    public P nullableDecimal(ToNullableDecimalFunction<T> func) {
        builder.addDecimal(func);
        return owner;
    }

    public P nullableDecimal(ToNullableDecimalFunction<T> func, JSONType encode) {
        builder.addDecimal(func, encode);
        this.childCompleted();
        return owner;
    }

    public P string(ToStringFunction<T> func) {
        builder.addString(func);
        this.childCompleted();
        return owner;
    }

    public P string(ToStringFunction<T> func, JSONType encode) {
        builder.addString(func, encode);
        this.childCompleted();
        return owner;
    }

    public P nullableString(ToStringFunction<T> func) {
        builder.addString(func);
        this.childCompleted();
        return owner;
    }

    public P nullableString(ToStringFunction<T> func, JSONType encode) {
        builder.addString(func, encode);
        this.childCompleted();
        return owner;
    }
}
