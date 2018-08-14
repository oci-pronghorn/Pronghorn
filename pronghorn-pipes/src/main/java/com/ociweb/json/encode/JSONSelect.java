package com.ociweb.json.encode;

import com.ociweb.json.encode.function.ToBoolFunction;

public abstract class JSONSelect<R, T, P> {
    private final JSONBuilder<R, T> builder;
    private int count = 0;
    private final ToBoolFunction<T>[] branches = new ToBoolFunction[512];
    private final JSONBuilder<R, T>[] cases = new JSONBuilder[512];

    public JSONSelect(JSONBuilder<R, T> builder) {
        this.builder = builder;
    }

    public JSONRoot<R, T, JSONSelect<R, T, P>> tryCase(ToBoolFunction<T> select) {
        JSONRoot<R, T, JSONSelect<R, T, P>> root = new JSONRoot<R, T, JSONSelect<R, T, P>>(builder.tryCase()) {
            @Override
            JSONSelect<R, T, P> rootEnded() {
                return JSONSelect.this;
            }
        };
        cases[count] = root.builder;
        branches[count] = select;
        count++;
        return root;
    }

    abstract P selectEnded();

    public P endSelect() {
        builder.endSelect(count, branches, cases);
        return selectEnded();
    }
}
