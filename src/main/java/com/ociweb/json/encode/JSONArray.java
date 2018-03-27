package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

import java.util.List;

public abstract class JSONArray<T, P, N> {
    private final JSONBuilder<T> builder;
    private final IteratorFunction<T, N> iterator;
    private final int depth;

    JSONArray(StringTemplateBuilder<T> scripts, JSONKeywords keywords, IteratorFunction<T, N> iterator, int depth) {
        this.iterator = iterator;
        this.depth = depth;
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
    }

    //@FunctionalInterface
    public static interface ArrayCompletion<P> {
        P end();
    }

    static <T, P, M, N> JSONArray<M, P, N> createArray(
            JSONBuilder<T> builder, int depth,
            final ToMemberFunction<T, M> accessor, // Convert parent T to iteratable M
            final IteratorFunction<M, N> iterator, // iterate of M using N
            final ArrayCompletion<P> ending) {
        return new JSONArray<M, P, N>(
                // called by builder to select null script
                builder.beginArray(accessor),
                builder.getKeywords(),
                // called by script to iterate over array given re-accessing M from T
                iterator,
                depth + 1) {
            @Override
            P arrayEnded() {
                return ending.end();
            }
        };
    }

    static <T, P, M extends List<N>, N> JSONArray<M, P, M> createListArray(
            JSONBuilder<T> builder, int depth,
            final ToMemberFunction<T, M> accessor,
            final ArrayCompletion<P> ending) {
        return new JSONArray<M, P, M>(
                builder.beginArray(accessor),
                builder.getKeywords(),
                new IteratorFunction<M, M>() {
                    @Override
                    public M get(M o, int i, M node) {
                        return i < o.size() ? o : null;
                    }
                },
                depth + 1) {
            @Override
            P arrayEnded() {
                return ending.end();
            }
        };
    }

    static <T, P, N> JSONArray<N[], P, N[]> createBasicArray(
            JSONBuilder<T> builder, int depth,
            final ToMemberFunction<T, N[]> accessor,
            final ArrayCompletion<P> ending) {
        return new JSONArray<N[], P, N[]>(
                builder.beginArray(accessor),
                builder.getKeywords(),
                new IteratorFunction<N[], N[]>() {
                    @Override
                    public N[] get(N[] o, int i, N[] node) {
                        return i < o.length ? o : null;
                    }
                },
                depth + 1) {
            @Override
            P arrayEnded() {
                return ending.end();
            }
        };
    }

    private P childCompleted() {
        builder.endArray();
        return arrayEnded();
    }

    abstract P arrayEnded();

    // Object

    public <M> JSONObject<M, P> beginObject(IterMemberFunction<T, M> accessor) {
        return new JSONObject<M, P>(
                builder.beginObject(iterator, accessor),
                builder.getKeywords(),depth + 1) {
            @Override
            P objectEnded() {
                return childCompleted();
            }
        };
    }

    // Array

    public <M, N2> JSONArray<M, P, N2> array(IterMemberFunction<T, M> accessor, IteratorFunction<M, N2> iterator) {
        return new JSONArray<M, P, N2>(
                builder.beginArray(this.iterator, accessor),
                builder.getKeywords(),
                iterator,
                depth + 1) {
            @Override
            P arrayEnded() {
                return childCompleted();
            }
        };
    }

    public <M extends List<N2>, N2> JSONArray<M, P, M> listArray(IterMemberFunction<T, M> accessor) {
        return new JSONArray<M, P, M>(
                builder.beginArray(this.iterator, accessor),
                builder.getKeywords(),
                new IteratorFunction<M, M>() {
                    @Override
                    public M get(M obj, int i, M node) {
                        return i < obj.size() ? obj : null;
                    }
                },
                depth + 1) {
            @Override
            P arrayEnded() {
                return childCompleted();
            }
        };
    }

    public <N2> JSONArray<N2[], P, N2[]> basicArray(IterMemberFunction<T, N2[]> accessor) {
        return new JSONArray<N2[], P, N2[]>(
                builder.beginArray(this.iterator, accessor),
                builder.getKeywords(),
                new IteratorFunction<N2[], N2[]>() {
                    @Override
                    public N2[] get(N2[] obj, int i, N2[] node) {
                        return i < obj.length ? obj : null;
                    }
                },
                depth + 1) {
            @Override
            P arrayEnded() {
                return childCompleted();
            }
        };
    }

    // Renderer

    public <M> P renderer(JSONRenderer<M> renderer, IterMemberFunction<T, M> accessor) {
        builder.addBuilder(iterator, renderer.builder, accessor);
        return this.childCompleted();
    }

    // Null

    public P empty() {
        return this.childCompleted();
    }

    public P constantNull() {
        builder.addNull(iterator);
        return this.childCompleted();
    }

    // Bool

    public P bool(IterBoolFunction<T> func) {
        builder.addBool(iterator, func);
        return this.childCompleted();
    }

    public P bool(IterBoolFunction<T> func, JSONType encode) {
        builder.addBool(iterator, func, encode);
        return this.childCompleted();
    }

    public P nullableBool(IterBoolFunction<T> isNull, IterBoolFunction<T> func) {
        builder.addBool(iterator, isNull, func);
        return this.childCompleted();
    }

    public P nullableBool(IterBoolFunction<T> isNull, IterBoolFunction<T> func, JSONType encode) {
        builder.addBool(iterator, isNull, func, encode);
        return this.childCompleted();
    }

    // Integer

    public P integer(IterLongFunction<T> func) {
        builder.addInteger(iterator, func);
        return this.childCompleted();
    }

    public P integer(IterLongFunction<T> func, JSONType encode) {
        builder.addInteger(iterator, func, encode);
        return this.childCompleted();
    }

    public P nullableInteger(IterBoolFunction<T> isNull, IterLongFunction<T> func) {
        builder.addInteger(iterator, isNull, func);
        return this.childCompleted();
    }

    public P nullableInteger(IterBoolFunction<T> isNull, IterLongFunction<T> func, JSONType encode) {
        builder.addInteger(iterator, isNull, func, encode);
        return this.childCompleted();
    }

    // Decimal

    public P decimal(int precision, IterDoubleFunction<T> func) {
        builder.addDecimal(iterator, precision, func);
        return this.childCompleted();
    }

    public P decimal(int precision, IterDoubleFunction<T> func, JSONType encode) {
        builder.addDecimal(iterator, precision, func, encode);
        return this.childCompleted();
    }

    public P nullableDecimal(int precision, IterBoolFunction<T> isNull, IterDoubleFunction<T> func) {
        builder.addDecimal(iterator, precision, isNull, func);
        return this.childCompleted();
    }

    public P nullableDecimal(int precision, IterBoolFunction<T> isNull, IterDoubleFunction<T> func, JSONType encode) {
        builder.addDecimal(iterator, precision, isNull, func, encode);
        return this.childCompleted();
    }

    // String

    public P string(IterStringFunction<T> func) {
        builder.addString(iterator, func);
        return this.childCompleted();
    }

    public P string(IterStringFunction<T> func, JSONType encode) {
        builder.addString(iterator, func, encode);
        return this.childCompleted();
    }

    public P nullableString(IterStringFunction<T> func) {
        builder.addNullableString(iterator, func);
        return this.childCompleted();
    }

    public P nullableString(IterStringFunction<T> func, JSONType encode) {
        builder.addNullableString(iterator, func, encode);
        return this.childCompleted();
    }
}
