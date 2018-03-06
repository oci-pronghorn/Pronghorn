package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

import java.util.List;

public abstract class JSONArray<T, P, N> {
    private final JSONBuilder<T> builder;
    private final ArrayIteratorFunction<T, N> iterator;
    private final int depth;

    JSONArray(StringTemplateBuilder<T> scripts, JSONKeywords keywords, ArrayIteratorFunction<T, N> iterator, int depth) {
        this.iterator = iterator;
        this.depth = depth;
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
    }

    static <T, P, N, M extends List<N>> JSONArray<T, P, N> createListArray(JSONBuilder<T> builder, int depth, ToMemberFunction<T, M> accessor, ToEnding<P> ending) {
        return new JSONArray<T, P, N>(
                builder.beginArray(new ToBoolFunction<T>() {
                    @Override
                    public boolean applyAsBool(T o) {
                        return accessor.get(o) == null;
                    }
                }),
                builder.getKeywords(),
                new ArrayIteratorFunction<T, N>() {
                    @Override
                    public N test(T o, int i, N node) {
                        List<N> m = accessor.get(o);
                        return i < m.size() ? m.get(i) : null;
                    }
                },
                depth + 1) {
            @Override
            P arrayEnded() {
                return ending.end();
            }
        };
    }

    static <T, P, N> JSONArray<T, P, N> createBasicArray(JSONBuilder<T> builder, int depth, ToMemberFunction<T, N[]> accessor, ToEnding<P> ending) {
        return new JSONArray<T, P, N>(
                builder.beginArray(new ToBoolFunction<T>() {
                    @Override
                    public boolean applyAsBool(T o) {
                        return accessor.get(o) == null;
                    }
                }),
                builder.getKeywords(),
                new ArrayIteratorFunction<T, N>() {
                    @Override
                    public N test(T o, int i, N node) {
                        N[] m = accessor.get(o);
                        return i < m.length ? m[i] : null;
                    }
                },
                depth + 1) {
            @Override
            P arrayEnded() {
                return ending.end();
            }
        };
    }

    private P endArray() {
        builder.endArray();
        return arrayEnded();
    }

    abstract P arrayEnded();

    // Object

    public <M> JSONObject<M, P> beginObject(IterMemberFunction<T, N, M> accessor) {
        return new JSONObject<M, P>(
                builder.beginObject(iterator, accessor),
                builder.getKeywords(),depth + 1) {
            @Override
            P objectEnded() {
                return endArray();
            }
        };
    }

    // Array

    // TODO

    // Renderer

    public <M> P renderer(JSONRenderer<M> renderer, IterMemberFunction<T, N, M> accessor) {
        builder.addRenderer(iterator, renderer, accessor);
        return this.endArray();
    }

    // Null

    public P constantNull() {
        builder.addNull(iterator);
        return this.endArray();
    }

    // TODO: nullable array elements for primitives

    // Bool

    public P bool(IterBoolFunction<T, N> func) {
        builder.addBool(iterator, func);
        return this.endArray();
    }

    public P bool(IterBoolFunction<T, N> func, JSONType encode) {
        builder.addBool(iterator, func, encode);
        return this.endArray();
    }

    // Integer

    public P integer(IterLongFunction<T, N> func) {
        builder.addInteger(iterator, func);
        return this.endArray();
    }

    public P integer(IterLongFunction<T, N> func, JSONType encode) {
        builder.addInteger(iterator, func, encode);
        return this.endArray();
    }

    @Deprecated
    public P integerNull(IterNullableLongFunction<T, N> func) {
        builder.addInteger(iterator, func);
        return this.endArray();
    }

    @Deprecated
    public P integerNull(IterNullableLongFunction<T, N> func, JSONType encode) {
        builder.addInteger(iterator, func, encode);
        return this.endArray();
    }

    // Decimal

    public P decimal(int precision, IterDoubleFunction<T, N> func) {
        builder.addDecimal(iterator, precision, func);
        return this.endArray();
    }

    public P decimal(int precision, IterDoubleFunction<T, N> func, JSONType encode) {
        builder.addDecimal(iterator, precision, func, encode);
        return this.endArray();
    }

    // String

    public P string(IterStringFunction<T, N> func) {
        builder.addString(iterator, func);
        return this.endArray();
    }

    public P string(IterStringFunction<T, N> func, JSONType encode) {
        builder.addString(iterator, func, encode);
        return this.endArray();
    }
}
