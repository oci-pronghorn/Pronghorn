package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

import java.util.Collection;
import java.util.List;

public class JSONRoot<T, P extends JSONRoot> implements JSONCompositeOwner {
    final JSONBuilder<T> builder;
    private final P owner;
    private final int depth;

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

    // Object

    public JSONObject<T, P> beginObject() {
        return beginObject(o->o);
    }

    public <M> JSONObject<M, P> beginObject(ToMemberFunction<T, M> accessor) {
        return new JSONObject<M, P>(
                builder.beginObject(accessor),
                builder.getKeywords(), owner, depth + 1);
    }

    // Array

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

    public <N, M extends List<N>> JSONArray<T, P, N> listArray(ToMemberFunction<T, M> accessor) {
        return new JSONArray<T, P, N>(
                builder.beginArray(new ToBoolFunction<T>() {
                    @Override
                    public boolean applyAsBool(T o) {
                        return accessor.apply(o) == null;
                    }
                }),
                builder.getKeywords(),
                new ArrayIteratorFunction<T, N>() {
                    @Override
                    public N test(T o, int i, N node) {
                        List<N> m = accessor.apply(o);
                        return i < m.size() ? m.get(i) : null;
                    }
                },
                owner, depth + 1);
    }

    public <N> JSONArray<T, P, N> basicArray(ToMemberFunction<T, N[]> accessor) {
        return new JSONArray<T, P, N>(
                builder.beginArray(new ToBoolFunction<T>() {
                    @Override
                    public boolean applyAsBool(T o) {
                        return accessor.apply(o) == null;
                    }
                }),
                builder.getKeywords(),
                new ArrayIteratorFunction<T, N>() {
                    @Override
                    public N test(T o, int i, N node) {
                        N[] m = accessor.apply(o);
                        return i < m.length ? m[i] : null;
                    }
                },
                owner, depth + 1);
    }

    // No need for Renderer methods

    // Null

    public P empty() {
        this.childCompleted();
        return owner;
    }

    public P constantNull() {
        builder.addNull();
        this.childCompleted();
        return owner;
    }

    // Bool

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

    public P nullableBool(ToBoolFunction<T> isNull, ToBoolFunction<T> func) {
        builder.addBool(isNull, func);
        this.childCompleted();
        return owner;
    }

    public P nullableBool(ToBoolFunction<T> isNull, ToBoolFunction<T> func, JSONType encode) {
        builder.addBool(isNull, func, encode);
        this.childCompleted();
        return owner;
    }

    // Integer

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

    public P nullableInteger(ToBoolFunction<T> isNull, ToLongFunction<T> func) {
        builder.addInteger(isNull, func);
        this.childCompleted();
        return owner;
    }

    // Decimal

    public P decimal(int precision, ToDoubleFunction<T> func) {
        builder.addDecimal(precision, func);
        this.childCompleted();
        return owner;
    }

    public P decimal(int precision, ToDoubleFunction<T> func, JSONType encode) {
        builder.addDecimal(precision, func, encode);
        this.childCompleted();
        return owner;
    }

    public P nullableDecimal(int precision, ToBoolFunction<T> isNull, ToDoubleFunction<T> func) {
        builder.addDecimal(precision, isNull, func);
        this.childCompleted();
        return owner;
    }

    public P nullableDecimal(int precision, ToBoolFunction<T> isNull, ToDoubleFunction<T> func, JSONType encode) {
        builder.addDecimal(precision, isNull, func, encode);
        this.childCompleted();
        return owner;
    }

    // String

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
        builder.addNullableString(func);
        this.childCompleted();
        return owner;
    }

    public P nullableString(ToStringFunction<T> func, JSONType encode) {
        builder.addNullableString(func, encode);
        this.childCompleted();
        return owner;
    }
}
