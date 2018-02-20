package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

public class JSONArray<T, P extends JSONCompositeOwner, N> implements JSONCompositeOwner {
    private final JSONBuilder<T> builder;
    private final ArrayIteratorFunction<T, N> iterator;
    private final P owner;
    private final int depth;

    JSONArray(StringTemplateBuilder<T> scripts, JSONKeywords keywords, ArrayIteratorFunction<T, N> iterator, P owner, int depth) {
        this.iterator = iterator;
        this.depth = depth;
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
        this.owner = owner;
    }

    @Override
    public void childCompleted() {
        // Single child...
        builder.endArray();
        owner.childCompleted();
    }

    // TODO: all other element types
    // TODO: use IterMemberFunction

    public <M> JSONObject<M, P> beginObject(ToMemberFunction<T, M> accessor) {
        return new JSONObject<M, P>(
                builder.beginObject(accessor),
                builder.getKeywords(), owner, depth + 1) {

            public P endObject() {
                builder.endObject();
                builder.endArray();
                owner.childCompleted();
                return owner;
            }
        };
    }

    public P constantNull() {
        builder.addNull(iterator);
        this.childCompleted();
        return owner;
    }

    public P integer(IterLongFunction<T, N> func) {
        builder.addInteger(iterator, func);
        this.childCompleted();
        return owner;
    }

    public P integer(IterLongFunction<T, N> func, JSONType encode) {
        builder.addInteger(iterator, func, encode);
        this.childCompleted();
        return owner;
    }

    public P integerNull(IterNullableLongFunction<T, N> func) {
        builder.addInteger(iterator, func);
        this.childCompleted();
        return owner;
    }

    public P integerNull(IterNullableLongFunction<T, N> func, JSONType encode) {
        builder.addInteger(iterator, func, encode);
        this.childCompleted();
        return owner;
    }
}
