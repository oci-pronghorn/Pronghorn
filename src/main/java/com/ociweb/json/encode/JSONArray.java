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

    // T, P, N are not the class's
    static <T, P, N, M> JSONArray<T, P, N> createArray(
            JSONBuilder<T> builder, int depth,
            final ToMemberFunction<T, M> accessor, // Convert parent T to iteratable M
            final IteratorFunction<M, N> iterator, // iterate of M using N
            final ArrayCompletion<P> ending) {
        return new JSONArray<T, P, N>(
                // called by builder to select null script
                builder.beginArray(accessor),
                builder.getKeywords(),
                // called by script to iterate over array given re-accessing M from T
                new IteratorFunction<T, N>() {
                    @Override
                    public N get(T o, int i, N node) {
                        M m = accessor.get(o);
                        return iterator.get(m, i, node);
                    }
                },
                depth + 1) {
            @Override
            P arrayEnded() {
                return ending.end();
            }
        };
    }

    static <T, P, N, M extends List<N>> JSONArray<T, P, M> createListArray(
            JSONBuilder<T> builder, int depth,
            final ToMemberFunction<T, M> accessor,
            final ArrayCompletion<P> ending) {
        return new JSONArray<T, P, M>(
                builder.beginArray(accessor),
                builder.getKeywords(),
                new IteratorFunction<T, M>() {
                    @Override
                    public M get(T o, int i, M node) {
                        M m = accessor.get(o);
                        return i < m.size() ? m : null;
                    }
                },
                depth + 1) {
            @Override
            P arrayEnded() {
                return ending.end();
            }
        };
    }

    static <T, P, N> JSONArray<T, P, N[]> createBasicArray(
            JSONBuilder<T> builder, int depth,
            final ToMemberFunction<T, N[]> accessor,
            final ArrayCompletion<P> ending) {
        return new JSONArray<T, P, N[]>(
                builder.beginArray(accessor),
                builder.getKeywords(),
                new IteratorFunction<T, N[]>() {
                    @Override
                    public N[] get(T o, int i, N[] node) {
                        N[] m = accessor.get(o);
                        return i < m.length ? m : null;
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
        builder.addRenderer(iterator, renderer, accessor);
        return this.childCompleted();
    }

    // Null

    public P constantNull() {
        builder.addNull(iterator);
        return this.childCompleted();
    }

    // TODO: nullable array elements for primitives

    // Bool

    public P bool(IterBoolFunction<T> func) {
        builder.addBool(iterator, func);
        return this.childCompleted();
    }

    public P bool(IterBoolFunction<T> func, JSONType encode) {
        builder.addBool(iterator, func, encode);
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

    @Deprecated
    public P integerNull(IterNullableLongFunction<T> func) {
        builder.addInteger(iterator, func);
        return this.childCompleted();
    }

    @Deprecated
    public P integerNull(IterNullableLongFunction<T> func, JSONType encode) {
        builder.addInteger(iterator, func, encode);
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

    // String

    public P string(IterStringFunction<T> func) {
        builder.addString(iterator, func);
        return this.childCompleted();
    }

    public P string(IterStringFunction<T> func, JSONType encode) {
        builder.addString(iterator, func, encode);
        return this.childCompleted();
    }
}
