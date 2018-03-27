package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

import java.util.List;

public abstract class JSONRoot<T, P> {
    final JSONBuilder<T> builder;
    private final int depth;

    JSONRoot(StringTemplateBuilder<T> scripts, JSONKeywords keywords, int depth) {
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
        this.depth = depth;
        builder.start();
    }

    abstract P rootEnded();

    private P childCompleted() {
        builder.complete();
        return rootEnded();
    }

    JSONBuilder<T> getBuilder() {
        return builder;
    }

    // Object

    public JSONObject<T, P> beginObject() {
        return beginObject(new ToMemberFunction<T, T>() {
            @Override
            public T get(T o) {
                return o;
            }
        });
    }

    public <M> JSONObject<M, P> beginObject(ToMemberFunction<T, M> accessor) {
        return new JSONObject<M, P>(
                builder.beginObject(accessor),
                builder.getKeywords(), depth + 1) {
            @Override
            P objectEnded() {
                return childCompleted();
            }
        };
    }

    // Array
    
    public <N> JSONArray<T, P, N> array(IteratorFunction<T, N> iterator) {
        return this.array(new ToMemberFunction<T, T>() {
            @Override
            public T get(T o) {
                return o;
            }
        }, iterator);
    }

    public <M, N> JSONArray<M, P, N> array(ToMemberFunction<T, M> accessor, IteratorFunction<M, N> iterator) {
        return JSONArray.createArray(builder, depth + 1, accessor, iterator, new JSONArray.ArrayCompletion<P>() {
            @Override
            public P end() {
                return childCompleted();
            }
        });
    }

    public <M extends List<N>, N> JSONArray<M, P, M> listArray(ToMemberFunction<T, M> accessor) {
        return JSONArray.createListArray(builder, depth + 1, accessor, new JSONArray.ArrayCompletion<P>() {
            @Override
            public P end() {
                return childCompleted();
            }
        });
    }

    public <N> JSONArray<N[], P, N[]> basicArray(ToMemberFunction<T, N[]> accessor) {
        return JSONArray.createBasicArray(builder, depth + 1, accessor, new JSONArray.ArrayCompletion<P>() {
            @Override
            public P end() {
                return childCompleted();
            }
        });
    }

    // No need for Renderer methods

    // Null

    public P empty() {
        return this.childCompleted();
    }

    public P constantNull() {
        builder.addNull();
        return this.childCompleted();
    }

    // Bool

    public P bool(ToBoolFunction<T> func) {
        builder.addBool(func);
        return this.childCompleted();
    }

    public P bool(ToBoolFunction<T> func, JSONType encode) {
        builder.addBool(func, encode);
        return this.childCompleted();
    }

    public P nullableBool(ToBoolFunction<T> isNull, ToBoolFunction<T> func) {
        builder.addBool(isNull, func);
        return this.childCompleted();
    }

    public P nullableBool(ToBoolFunction<T> isNull, ToBoolFunction<T> func, JSONType encode) {
        builder.addBool(isNull, func, encode);
        return this.childCompleted();
    }

    // Integer

    public P integer(ToLongFunction<T> func) {
        builder.addInteger(func);
        return this.childCompleted();
    }

    public P integer(ToLongFunction<T> func, JSONType encode) {
        builder.addInteger(func, encode);
        return this.childCompleted();
    }

    public P nullableInteger(ToBoolFunction<T> isNull, ToLongFunction<T> func) {
        builder.addInteger(isNull, func);
        return this.childCompleted();
    }

    public P nullableInteger(ToBoolFunction<T> isNull, ToLongFunction<T> func, JSONType encode) {
        builder.addInteger(isNull, func, encode);
        return this.childCompleted();
    }

    // Decimal

    public P decimal(int precision, ToDoubleFunction<T> func) {
        builder.addDecimal(precision, func);
        return this.childCompleted();
    }

    public P decimal(int precision, ToDoubleFunction<T> func, JSONType encode) {
        builder.addDecimal(precision, func, encode);
        return this.childCompleted();
    }

    public P nullableDecimal(int precision, ToBoolFunction<T> isNull, ToDoubleFunction<T> func) {
        builder.addDecimal(precision, isNull, func);
        return this.childCompleted();
    }

    public P nullableDecimal(int precision, ToBoolFunction<T> isNull, ToDoubleFunction<T> func, JSONType encode) {
        builder.addDecimal(precision, isNull, func, encode);
        return this.childCompleted();
    }

    // String

    public P string(ToStringFunction<T> func) {
        builder.addString(func);
        return this.childCompleted();
    }

    public P string(ToStringFunction<T> func, JSONType encode) {
        builder.addString(func, encode);
        return this.childCompleted();
    }

    public P nullableString(ToStringFunction<T> func) {
        builder.addNullableString(func);
        return this.childCompleted();
    }

    public P nullableString(ToStringFunction<T> func, JSONType encode) {
        builder.addNullableString(func, encode);
        return this.childCompleted();
    }
}
