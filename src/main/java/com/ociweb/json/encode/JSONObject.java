package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

import java.util.List;

public abstract class JSONObject<T, P> {
    private final JSONBuilder<T> builder;
    private final int depth;

    JSONObject(StringTemplateBuilder<T> scripts, JSONKeywords keywords, int depth) {
        this.depth = depth;
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
    }

    public P endObject() {
        builder.endObject();
        return objectEnded();
    }

    abstract P objectEnded();

    // Object

    public JSONObject<T, JSONObject<T, P>> beginObject(String name) {
        return beginObject(name, new ToMemberFunction<T, T>() {
            @Override
            public T get(T o) {
                return o;
            }
        });
    }

    public <M> JSONObject<M, JSONObject<T, P>> beginObject(String name, ToMemberFunction<T, M> accessor) {
        return new JSONObject<M, JSONObject<T, P>>(
                builder.addFieldPrefix(name).beginObject(accessor),
                builder.getKeywords(),depth + 1) {
            @Override
            JSONObject<T, P> objectEnded() {
                return JSONObject.this;
            }
        };
    }

    // Array

    public <N> JSONArray<T, JSONObject<T, P>, N> array(String name, IterMemberFunction<T, N, N> iterator) {
        return array(name, new ToMemberFunction<T, T>() {
            @Override
            public T get(T o) {
                return o;
            }
        }, iterator);
    }

    public <N, M> JSONArray<T, JSONObject<T, P>, N> array(String name, ToMemberFunction<T, M> accessor, IterMemberFunction<M, N, N> iterator) {
        return JSONArray.createArray(builder.addFieldPrefix(name), depth + 1, accessor, iterator,  new ToEnding<JSONObject<T, P>>() {
            @Override
            public JSONObject<T, P> end() {
                return JSONObject.this;
            }
        });
    }

    public <N, M extends List<N>> JSONArray<T, JSONObject<T, P>, N> listArray(String name, ToMemberFunction<T, M> accessor) {
        return JSONArray.createListArray(builder.addFieldPrefix(name), depth + 1, accessor, new ToEnding<JSONObject<T, P>>() {
            @Override
            public JSONObject<T, P> end() {
                return JSONObject.this;
            }
        });
    }

    public <N> JSONArray<T, JSONObject<T, P>, N> basicArray(String name, ToMemberFunction<T, N[]> accessor) {
        return JSONArray.createBasicArray(builder.addFieldPrefix(name), depth + 1, accessor, new ToEnding<JSONObject<T, P>>() {
            @Override
            public JSONObject<T, P> end() {
                return JSONObject.this;
            }
        });
    }

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
