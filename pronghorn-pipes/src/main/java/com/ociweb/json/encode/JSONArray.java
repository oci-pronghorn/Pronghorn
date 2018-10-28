package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @param <R> Root of renderer
 * @param <T> Data source type
 * @param <P> Builder return type
 * @param <N> Iterating node
 */
public abstract class JSONArray<R, T, P, N> {
    final JSONBuilder<R, T> builder;
    private final IteratorFunction<T, N> iterator;

    JSONArray(JSONBuilder<R, T> builder, IteratorFunction<T, N> iterator) {
        this.builder = builder;
        this.iterator = iterator;
    }

    //@FunctionalInterface
    public static interface ArrayCompletion<P> {
        P end();
    }

    static <R, T, P, M, N> JSONArray<R, M, P, N> createArray(
            JSONBuilder<R, T> builder,
            final ToMemberFunction<T, M> accessor, // Convert parent T to iteratable M
            final IteratorFunction<M, N> iterator, // iterate of M using N
            final ArrayCompletion<P> ending) {
        return new JSONArray<R, M, P, N>(
                // called by builder to select null script
                builder.beginArray(accessor),
                // called by script to iterate over array given re-accessing M from T
                iterator) {
            @Override
            P arrayEnded() {
                return ending.end();
            }
        };
    }

    static <R, T, P, M extends List<N>, N> JSONArray<R, M, P, M> createListArray(
            JSONBuilder<R, T> builder,
            final ToMemberFunction<T, M> accessor,
            final ArrayCompletion<P> ending) {
        return new JSONArray<R, M, P, M>(
                builder.beginArray(accessor),
                new IteratorFunction<M, M>() {
                    @Override
                    public M get(M o, int i) {
                        return i < o.size() ? o : null;
                    }
                }) {
            @Override
            P arrayEnded() {
                return ending.end();
            }
        };
    }

    static <R, T, P, N> JSONArray<R, N[], P, N[]> createBasicArray(
            JSONBuilder<R, T> builder,
            final ToMemberFunction<T, N[]> accessor,
            final ArrayCompletion<P> ending) {
        return new JSONArray<R, N[], P, N[]>(
                builder.beginArray(accessor),
                new IteratorFunction<N[], N[]>() {
                    @Override
                    public N[] get(N[] o, int i) {
                        return i < o.length ? o : null;
                    }
                }) {
            @Override
            P arrayEnded() {
                return ending.end();
            }
        };
    }

    static <R, T, P, M extends Collection<N>, N> JSONArray<R, Iterator<N>, P, Iterator<N>> createCollectionArray(
            JSONBuilder<R, T> builder,
            final ToMemberFunction<T, M> accessor,
            final ArrayCompletion<P> ending) {
        return new JSONArray<R, Iterator<N>, P, Iterator<N>>(
                builder.beginArray(new ToMemberFunction<T, Iterator<N>>() {
                    @Override
                    public Iterator<N> get(T o) {
                        Collection<N> collection = accessor.get(o);
                        return collection != null ? collection.iterator() : null;
                    }
                }),
                new IteratorFunction<Iterator<N>, Iterator<N>>() {
                    @Override
                    public Iterator<N> get(Iterator<N> o, int i) {
                        return o.hasNext() ? o : null;
                    }
                }) {
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

    @Deprecated
    public <M> JSONObject<R, M, P> beginObject(IterMemberFunction<T, M> accessor) {
        return new JSONObject<R, M, P>(builder.beginObject(iterator, accessor)) {
            @Override
            P objectEnded() {
                return childCompleted();
            }
        };
    }
    
    public <M> JSONObject<R, M, P> startObject(IterMemberFunction<T, M> accessor) {
        return new JSONObject<R, M, P>(builder.beginObject(iterator, accessor)) {
            @Override
            P objectEnded() {
                return childCompleted();
            }
        };
    }

    // Array

    public <M, N2> JSONArray<R, M, P, N2> array(IterMemberFunction<T, M> accessor, IteratorFunction<M, N2> iterator) {
        return new JSONArray<R, M, P, N2>(builder.beginArray(this.iterator, accessor), iterator) {
            @Override
            P arrayEnded() {
                return childCompleted();
            }
        };
    }

    public <M extends List<N2>, N2> JSONArray<R, M, P, M> listArray(IterMemberFunction<T, M> accessor) {
        return new JSONArray<R, M, P, M>(
                builder.beginArray(this.iterator, accessor),
                new IteratorFunction<M, M>() {
                    @Override
                    public M get(M obj, int i) {
                        return i < obj.size() ? obj : null;
                    }
                }) {
            @Override
            P arrayEnded() {
                return childCompleted();
            }
        };
    }

    public <N2> JSONArray<R, N2[], P, N2[]> basicArray(IterMemberFunction<T, N2[]> accessor) {
        return new JSONArray<R, N2[], P, N2[]>(
                builder.beginArray(this.iterator, accessor),
                new IteratorFunction<N2[], N2[]>() {
                    @Override
                    public N2[] get(N2[] obj, int i) {
                        return i < obj.length ? obj : null;
                    }
                }) {
            @Override
            P arrayEnded() {
                return childCompleted();
            }
        };
    }

    public <M extends Collection<N2>, N2> JSONArray<R, Iterator<N2>, P, Iterator<N2>> iterArray(final IterMemberFunction<T, M> accessor) {
        return new JSONArray<R, Iterator<N2>, P, Iterator<N2>>(
                builder.beginArray(this.iterator, new IterMemberFunction<T, Iterator<N2>>() {
                    @Override
                    public Iterator<N2> get(T o, int i) {
                        Collection<N2> m = accessor.get(o, i);
                        return m != null ? m.iterator() : null;
                    }
                }),
                new IteratorFunction<Iterator<N2>, Iterator<N2>>() {
                    @Override
                    public Iterator<N2> get(Iterator<N2> obj, int i) {
                        return obj.hasNext() ? obj : null;
                    }
                }) {
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
/*
    public P recurseRoot(IterMemberFunction<T, R> accessor) {
        builder.recurseRoot(iterator, accessor);
        return this.childCompleted();
    }
*/
    public JSONArraySelect<R, T, P, N> beginSelect() {
        return new JSONArraySelect<R, T, P, N>(builder.beginSelect(), iterator) {
            @Override
            P selectEnded() {
                return childCompleted();
            }
        };
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
        builder.addBool(iterator, null, func);
        return this.childCompleted();
    }

    public P bool(IterBoolFunction<T> func, JSONType encode) {
        builder.addBool(iterator, null, func, encode);
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
        builder.addInteger(iterator, null, func);
        return this.childCompleted();
    }

    public P integer(IterLongFunction<T> func, JSONType encode) {
        builder.addInteger(iterator, null, func, encode);
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
        builder.addDecimal(iterator, precision, null, func);
        return this.childCompleted();
    }

    public P decimal(int precision, IterDoubleFunction<T> func, JSONType encode) {
        builder.addDecimal(iterator, precision, null, func, encode);
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
        builder.addString(iterator, false, func);
        return this.childCompleted();
    }

    public P string(IterStringFunction<T> func, JSONType encode) {
        builder.addString(iterator, false, func, encode);
        return this.childCompleted();
    }

    public P nullableString(IterStringFunction<T> func) {
        builder.addString(iterator, true, func);
        return this.childCompleted();
    }

    public P nullableString(IterStringFunction<T> func, JSONType encode) {
        builder.addString(iterator, true, func, encode);
        return this.childCompleted();
    }

    // Enum

    public <E extends Enum<E>> P enumName(IterEnumFunction<T, E> func) {
        builder.addEnumName(iterator, func);
        return this.childCompleted();
    }

    public <E extends Enum<E>> P enumOrdinal(IterEnumFunction<T, E> func) {
        builder.addEnumOrdinal(iterator, func);
        return this.childCompleted();
    }
}
