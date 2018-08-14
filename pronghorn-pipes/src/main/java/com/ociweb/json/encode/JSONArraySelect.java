package com.ociweb.json.encode;

import com.ociweb.json.encode.function.IterBoolFunction;
import com.ociweb.json.encode.function.IteratorFunction;

public abstract class JSONArraySelect<R, T, P, N> {
    private final JSONBuilder<R, T> builder;
    private final IteratorFunction<T, N> iterator;
    private int count = 0;
    private final IterBoolFunction<T>[] branches = new IterBoolFunction[512];
    private final JSONBuilder<R, T>[] cases = new JSONBuilder[512];

    public JSONArraySelect(JSONBuilder<R, T> builder, IteratorFunction<T, N> iterator) {
        this.builder = builder;
        this.iterator = iterator;
    }

    public JSONArray<R, T, JSONArraySelect<R, T, P, N>, N> tryCase(IterBoolFunction<T> select) {
        JSONArray<R, T, JSONArraySelect<R, T, P, N>, N> root = new JSONArray<R, T, JSONArraySelect<R, T, P, N>, N>(builder.tryCase(), iterator) {
            @Override
            JSONArraySelect<R, T, P, N> arrayEnded() {
                return JSONArraySelect.this;
            }
        };
        cases[count] = root.builder;
        branches[count] = select;
        count++;
        return root;
    }

    abstract P selectEnded();

    public P endSelect() {
        builder.endSelect(iterator, count, branches, cases);
        return selectEnded();
    }
}
