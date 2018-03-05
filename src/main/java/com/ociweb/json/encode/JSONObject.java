package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

public class JSONObject<T, P extends JSONCompositeOwner> implements JSONCompositeOwner {
    private final JSONBuilder<T> builder;
    private final P owner;
    private final int depth;

    JSONObject(StringTemplateBuilder<T> scripts, JSONKeywords keywords, P owner, int depth) {
        this.depth = depth;
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
        this.owner = owner;
    }

    // TODO: may not be necessary after endObject() refactor
    @Override
    public void childCompleted() {
        // does not matter
    }

    // TODO: make abstract and force impl on new
    public P endObject() {
        builder.endObject();
        owner.childCompleted();
        return owner;
    }

    // Object

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

    // Array

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

    // TODO: add array convenience methods

    // Renderer

    // TODO: recursive renderer

    public <M> JSONObject<T, P> renderer(String name, JSONRenderer<M> renderer, ToMemberFunction<T, M> accessor) {
        builder.addFieldPrefix(name).addRenderer(renderer, accessor);
        return this;
    }

    // No need for empty()

    // Null

    public JSONObject<T, P> constantNull(String name) {
        builder.addFieldPrefix(name).addNull();
        return this;
    }

    // Boolean

    public JSONObject<T, P> bool(String name, ToBoolFunction<T> func) {
        builder.addFieldPrefix(name).addBool(func);
        return this;
    }

    public JSONObject<T, P> bool(String name, ToBoolFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addBool(func, encode);
        return this;
    }

    public JSONObject<T, P> nullableBool(String name, ToBoolFunction<T> isNull, ToBoolFunction<T> func) {
        builder.addFieldPrefix(name).addBool(isNull, func);
        return this;
    }

    public JSONObject<T, P> nullableBool(String name, ToBoolFunction<T> isNull, ToBoolFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addBool(isNull, func, encode);
        return this;
    }

    @Deprecated
    public JSONObject<T, P> nullableBool(String name, ToNullableBoolFunction<T> func) {
        builder.addFieldPrefix(name).addBool(func);
        return this;
    }

    @Deprecated
    public JSONObject<T, P> nullableBool(String name, ToNullableBoolFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addBool(func, encode);
        return this;
    }

    // Integer

    public JSONObject<T, P> integer(String name, ToLongFunction<T> func) {
        builder.addFieldPrefix(name).addInteger(func);
        return this;
    }

    public JSONObject<T, P> integer(String name, ToLongFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addInteger(func, encode);
        return this;
    }

    public JSONObject<T, P> nullableInteger(String name, ToBoolFunction<T> isNull, ToLongFunction<T> func) {
        builder.addFieldPrefix(name).addInteger(isNull, func);
        return this;
    }

    public JSONObject<T, P> nullableInteger(String name, ToBoolFunction<T> isNull, ToLongFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addInteger(isNull, func, encode);
        return this;
    }

    @Deprecated
    public JSONObject<T, P> nullableInteger(String name, ToNullableLongFunction<T> func) {
        builder.addFieldPrefix(name).addInteger(func);
        return this;
    }

    @Deprecated
    public JSONObject<T, P> nullableInteger(String name, ToNullableLongFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addInteger(func, encode);
        return this;
    }

    // Decimal

    public JSONObject<T, P> decimal(String name, int precision, ToDoubleFunction<T> func) {
        builder.addFieldPrefix(name).addDecimal(precision, func);
        return this;
    }

    public JSONObject<T, P> decimal(String name, int precision, ToDoubleFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addDecimal(precision, func, encode);
        return this;
    }

    public JSONObject<T, P> nullableDecimal(String name, int precision, ToBoolFunction<T> isNull, ToDoubleFunction<T> func) {
        builder.addFieldPrefix(name).addDecimal(precision, isNull, func);
        return this;
    }

    public JSONObject<T, P> nullableDecimal(String name, int precision, ToBoolFunction<T> isNull, ToDoubleFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addDecimal(precision, isNull, func, encode);
        return this;
    }

    @Deprecated
    public JSONObject<T, P> decimal(String name, ToDecimalFunction<T> func) {
        builder.addFieldPrefix(name).addDecimal(func);
        return this;
    }

    @Deprecated
    public JSONObject<T, P> decimal(String name, ToDecimalFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addDecimal(func, encode);
        return this;
    }

    @Deprecated
    public JSONObject<T, P> nullableDecimal(String name, ToBoolFunction<T> isNull, ToDecimalFunction<T> func) {
        builder.addFieldPrefix(name).addDecimal(isNull, func);
        return this;
    }

    @Deprecated
    public JSONObject<T, P> nullableDecimal(String name, ToBoolFunction<T> isNull, ToDecimalFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addDecimal(isNull, func, encode);
        return this;
    }

    @Deprecated
    public JSONObject<T, P> nullableDecimal(String name, ToNullableDecimalFunction<T> func) {
        builder.addFieldPrefix(name).addDecimal(func);
        return this;
    }

    @Deprecated
    public JSONObject<T, P> nullableDecimal(String name, ToNullableDecimalFunction<T> func, JSONType encode) {
        builder.addFieldPrefix(name).addDecimal(func, encode);
        return this;
    }

    // String

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
}
