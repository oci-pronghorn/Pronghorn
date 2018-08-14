package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public abstract class JSONRoot<R, T, P> {
    protected final JSONBuilder<R, T> builder;

    JSONRoot(JSONBuilder<R, T> builder) {
        this.builder = builder;
        builder.start();
    }

    abstract P rootEnded();

    private P childCompleted() {
        builder.complete();
        return rootEnded();
    }

    // Object

    public JSONObject<R, T, P> beginObject() {
        return beginObject(new ToMemberFunction<T, T>() {
            @Override
            public T get(T o) {
                return o;
            }
        });
    }

    public <M> JSONObject<R, M, P> beginObject(ToMemberFunction<T, M> accessor) {
        return new JSONObject<R, M, P>(builder.beginObject(accessor)) {
            @Override
            P objectEnded() {
                return childCompleted();
            }
        };
    }

    // Array
    
    public <N> JSONArray<R, T, P, N> array(IteratorFunction<T, N> iterator) {
        return this.array(new ToMemberFunction<T, T>() {
            @Override
            public T get(T o) {
                return o;
            }
        }, iterator);
    }

    public <M, N> JSONArray<R, M, P, N> array(ToMemberFunction<T, M> accessor, IteratorFunction<M, N> iterator) {
        return JSONArray.createArray(builder, accessor, iterator, new JSONArray.ArrayCompletion<P>() {
            @Override
            public P end() {
                return childCompleted();
            }
        });
    }

    public <M extends List<N>, N> JSONArray<R, M, P, M> listArray(ToMemberFunction<T, M> accessor) {
        return JSONArray.createListArray(builder, accessor, new JSONArray.ArrayCompletion<P>() {
            @Override
            public P end() {
                return childCompleted();
            }
        });
    }

    public <N> JSONArray<R, N[], P, N[]> basicArray(ToMemberFunction<T, N[]> accessor) {
        return JSONArray.createBasicArray(builder, accessor, new JSONArray.ArrayCompletion<P>() {
            @Override
            public P end() {
                return childCompleted();
            }
        });
    }

    public <M extends Collection<N>, N> JSONArray<R, Iterator<N>, P, Iterator<N>> iterArray(ToMemberFunction<T, M> accessor) {
        return JSONArray.createCollectionArray(builder, accessor, new JSONArray.ArrayCompletion<P>() {
            @Override
            public P end() {
                return childCompleted();
            }
        });
    }

    // Renderer

    public <M> P renderer(JSONRenderer<M> renderer, ToMemberFunction<T, M> accessor) {
        builder.addBuilder(renderer.builder, accessor);
        return this.childCompleted();
    }

    public <M> P recurseRoot(ToMemberFunction<T, R> accessor) {
        builder.recurseRoot(accessor);
        return this.childCompleted();
    }

    public JSONSelect<R, T, P> beginSelect() {
        return new JSONSelect<R, T, P>(builder.beginSelect()) {
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
        builder.addNull();
        return this.childCompleted();
    }

    // Bool

    public P bool(ToBoolFunction<T> func) {
        builder.addBool(null, func);
        return this.childCompleted();
    }

    public P bool(ToBoolFunction<T> func, JSONType encode) {
        builder.addBool(null, func, encode);
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
        builder.addInteger(null, func);
        return this.childCompleted();
    }

    public P integer(ToLongFunction<T> func, JSONType encode) {
        builder.addInteger(null, func, encode);
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
        builder.addDecimal(precision, null, func);
        return this.childCompleted();
    }

    public P decimal(int precision, ToDoubleFunction<T> func, JSONType encode) {
        builder.addDecimal(precision, null, func, encode);
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
        builder.addString(false, func);
        return this.childCompleted();
    }

    public P string(ToStringFunction<T> func, JSONType encode) {
        builder.addString(false, func, encode);
        return this.childCompleted();
    }

    public P nullableString(final ToStringFunction<T> func) {
        builder.addString(true, func);
        return this.childCompleted();
    }

    public P nullableString(ToStringFunction<T> func, JSONType encode) {
        builder.addString(true, func, encode);
        return this.childCompleted();
    }

    // Enum

    public <E extends Enum<E>> P enumName(ToEnumFunction<T, E> func) {
        builder.addEnumName(func);
        return this.childCompleted();
    }

    public <E extends Enum<E>> P enumOrdinal(ToEnumFunction<T, E> func) {
        builder.addEnumOrdinal(func);
        return this.childCompleted();
    }
}
