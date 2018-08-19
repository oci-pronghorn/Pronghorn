package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public abstract class JSONObject<R, T, P> {
    private final JSONBuilder<R, T> builder;
    private boolean declaredEmpty = false;

    JSONObject(JSONBuilder<R, T> builder) {
        this.builder = builder;
    }

    public P endObject() {
        builder.endObject();
        return objectEnded();
    }

    abstract P objectEnded();

    // Object

    @Deprecated //use startObject
    public JSONObject<R, T, JSONObject<R, T, P>> beginObject(String name) {
        return startObject(name);
    }

    @Deprecated //use startObject
    public <M> JSONObject<R, M, JSONObject<R, T, P>> beginObject(String name, ToMemberFunction<T, M> accessor) {
        return startObject(name, accessor);
    }
    
    public JSONObject<R, T, JSONObject<R, T, P>> startObject(String name) {
        return startObject(name, new ToMemberFunction<T, T>() {
            @Override
            public T get(T o) {
                return o;
            }
        });
    }

    public <M> JSONObject<R, M, JSONObject<R, T, P>> startObject(String name, ToMemberFunction<T, M> accessor) {
        return new JSONObject<R, M, JSONObject<R, T, P>>(builder.addFieldPrefix(name).beginObject(accessor)) {
            @Override
            JSONObject<R, T, P> objectEnded() {
                return JSONObject.this;
            }
        };
    }

    // Array
    
    public <N> JSONArray<R, T, JSONObject<R, T, P>, N> array(String name, IteratorFunction<T, N> iterator) {
        return this.array(name, new ToMemberFunction<T, T>() {
            @Override
            public T get(T o) {
                return o;
            }
        }, iterator);
    }

    public <M, N> JSONArray<R, M, JSONObject<R, T, P>, N> array(String name, ToMemberFunction<T, M> accessor, IteratorFunction<M, N> iterator) {
        return JSONArray.createArray(builder.addFieldPrefix(name), accessor, iterator,  new JSONArray.ArrayCompletion<JSONObject<R, T, P>>() {
            @Override
            public JSONObject<R, T, P> end() {
                return JSONObject.this;
            }
        });
    }

    public <M extends List<N>, N> JSONArray<R, M, JSONObject<R, T, P>, M> listArray(String name, ToMemberFunction<T, M> accessor) {
        return JSONArray.createListArray(builder.addFieldPrefix(name), accessor, new JSONArray.ArrayCompletion<JSONObject<R, T, P>>() {
            @Override
            public JSONObject<R, T, P> end() {
                return JSONObject.this;
            }
        });
    }

    public <N> JSONArray<R, N[], JSONObject<R, T, P>, N[]> basicArray(String name, ToMemberFunction<T, N[]> accessor) {
        return JSONArray.createBasicArray(builder.addFieldPrefix(name), accessor, new JSONArray.ArrayCompletion<JSONObject<R, T, P>>() {
            @Override
            public JSONObject<R, T, P> end() {
                return JSONObject.this;
            }
        });
    }

    public <M extends Collection<N>, N> JSONArray<R, Iterator<N>, JSONObject<R, T, P>, Iterator<N>> iterArray(String name, ToMemberFunction<T, M> accessor) {
        return JSONArray.createCollectionArray(builder.addFieldPrefix(name), accessor, new JSONArray.ArrayCompletion<JSONObject<R, T, P>>() {
            @Override
            public JSONObject<R, T, P> end() {
                return JSONObject.this;
            }
        });
    }

    // Renderer

    public <M> JSONObject<R, T, P> renderer(String name, JSONRenderer<M> renderer, ToMemberFunction<T, M> accessor) {
        builder.addFieldPrefix(name).addBuilder(renderer.builder, accessor);
        return this;
    }

    public JSONObject<R, T, P> recurseRoot(String name, ToMemberFunction<T, R> accessor) {
        builder.addFieldPrefix(name).recurseRoot(accessor);
        return this;
    }

    public JSONSelect<R, T, JSONObject<R, T, P>> beginSelect(String name) {
        return new JSONSelect<R, T, JSONObject<R, T, P>>(builder.addFieldPrefix(name).beginSelect()) {
            @Override
            JSONObject<R, T, P> selectEnded() {
                return JSONObject.this;
            }
        };
    }

    // Null

    public JSONObject<R, T, P> constantNull(String name) {
        builder.addFieldPrefix(name).addNull();
        return this;
    }

    // Allow for documented empty
    public JSONObject<R, T, P> empty() {
        //assert(objectElementIndex == -1) : "empty can only be called on empty object";
        declaredEmpty = true;
        return this;
    }

    // Boolean

    public JSONObject<R, T, P> bool(String name, ToBoolFunction<T> func) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addBool(null, func);
        return this;
    }

    public JSONObject<R, T, P> bool(String name, ToBoolFunction<T> func, JSONType encode) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addBool(null, func, encode);
        return this;
    }

    public JSONObject<R, T, P> nullableBool(String name, ToBoolFunction<T> isNull, ToBoolFunction<T> func) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addBool(isNull, func);
        return this;
    }

    public JSONObject<R, T, P> nullableBool(String name, ToBoolFunction<T> isNull, ToBoolFunction<T> func, JSONType encode) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addBool(isNull, func, encode);
        return this;
    }

    // Integer

    public JSONObject<R, T, P> integer(String name, ToLongFunction<T> func) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addInteger(null, func);
        return this;
    }

    public JSONObject<R, T, P> integer(String name, ToLongFunction<T> func, JSONType encode) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addInteger(null, func, encode);
        return this;
    }

    public JSONObject<R, T, P> nullableInteger(String name, ToBoolFunction<T> isNull, ToLongFunction<T> func) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addInteger(isNull, func);
        return this;
    }

    public JSONObject<R, T, P> nullableInteger(String name, ToBoolFunction<T> isNull, ToLongFunction<T> func, JSONType encode) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addInteger(isNull, func, encode);
        return this;
    }

    // Decimal

    public JSONObject<R, T, P> decimal(String name, int precision, ToDoubleFunction<T> func) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addDecimal(precision, null, func);
        return this;
    }

    public JSONObject<R, T, P> decimal(String name, int precision, ToDoubleFunction<T> func, JSONType encode) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addDecimal(precision, null, func, encode);
        return this;
    }

    public JSONObject<R, T, P> nullableDecimal(String name, int precision, ToBoolFunction<T> isNull, ToDoubleFunction<T> func) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addDecimal(precision, isNull, func);
        return this;
    }

    public JSONObject<R, T, P> nullableDecimal(String name, int precision, ToBoolFunction<T> isNull, ToDoubleFunction<T> func, JSONType encode) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addDecimal(precision, isNull, func, encode);
        return this;
    }

    // String

    public JSONObject<R, T, P> string(String name, ToStringFunction<T> func) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addString(false, func);
        return this;
    }

    public JSONObject<R, T, P> string(String name, ToStringFunction<T> func, JSONType encode) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addString(false, func, encode);
        return this;
    }

    public JSONObject<R, T, P> nullableString(String name, ToStringFunction<T> func) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addString(true, func);
        return this;
    }

    public JSONObject<R, T, P> nullableString(String name, ToStringFunction<T> func, JSONType encode) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addString(true, func, encode);
        return this;
    }

    // Enum

    public <E extends Enum<E>> JSONObject<R, T, P> enumName(String name, ToEnumFunction<T, E> func) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addEnumName(func);
        return this;
    }

    public <E extends Enum<E>> JSONObject<R, T, P> enumOrdinal(String name, ToEnumFunction<T, E> func) {
        assert(!declaredEmpty);
        builder.addFieldPrefix(name).addEnumOrdinal(func);
        return this;
    }
}
