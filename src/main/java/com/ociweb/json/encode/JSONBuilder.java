package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.encode.template.StringTemplateBuilder;
import com.ociweb.json.encode.appendable.AppendableByteWriter;
import com.ociweb.pronghorn.util.Appendables;

import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

class JSONBuilder<T> {
    private final StringTemplateBuilder<T> scripts;

    private final JSONKeywords kw;
    private final int depth;
    private final StringTemplateBuilder<T>[] nullableBranches = new StringTemplateBuilder[2];
    private int objectElementIndex = 0;
    private ToIntFunction<T> arrayLength;

    JSONBuilder(StringTemplateBuilder<T> scripts, JSONKeywords kw, int depth) {
        this.scripts = scripts;
        this.kw = kw;
        this.depth = depth;
        StringTemplateBuilder<T> objNullBranch = new StringTemplateBuilder<>();
        kw.Null(objNullBranch);
        objNullBranch.lock();
        nullableBranches[0] = objNullBranch;
    }

    JSONKeywords getKeywords() {
        return kw;
    }

    void setArrayLength(ToIntFunction<T> arrayLength) {
        this.arrayLength = arrayLength;
    }

    void start() {
        kw.Start(scripts);
    }

    void complete() {
        kw.Complete(scripts);
        if (nullableBranches[1] != null) nullableBranches[1].lock();
        scripts.lock();
        nullableBranches[0] = null;
        nullableBranches[1] = null;
        arrayLength = null;
    }

    void render(AppendableByteWriter writer, T source) {
        scripts.render(writer, source);
    }

    // Helper

    JSONBuilder<T> addFieldPrefix(String name) {
        if (objectElementIndex == 0) {
            kw.FirstObjectElement(scripts, depth);
        }
        else {
            kw.NextObjectElement(scripts, depth);
        }
        scripts.add(name);
        kw.ObjectValue(scripts, depth);
        return this;
    }

    // Object

    StringTemplateBuilder<T>  beginObject() {
        kw.OpenObj(scripts, depth);
        return scripts;
    }

    StringTemplateBuilder<T> beginObject(ToBoolFunction<T> isNull) {
        StringTemplateBuilder<T> notNullBranch = new StringTemplateBuilder<>();
        kw.OpenObj(notNullBranch, depth);
        nullableBranches[1] = notNullBranch;
        scripts.add(nullableBranches, o -> isNull.applyAsBool(o) ? 0 : 1);
        return notNullBranch;
    }

    void endObject() {
        kw.CloseObj(scripts, depth);
    }

    // Array

    StringTemplateBuilder<T> beginArray() {
        kw.OpenArray(scripts, depth);
        return scripts;
    }

    StringTemplateBuilder<T> beginArray(ToIntFunction<T> length) {
        StringTemplateBuilder<T> notNullBranch = new StringTemplateBuilder<>();
        kw.OpenArray(notNullBranch, depth);
        nullableBranches[1] = notNullBranch;
        scripts.add(nullableBranches, o -> length.applyAsInt(o) < 0 ? 0 : 1);
        return notNullBranch;
    }

    // Bool

    void addNull() {
        if (this.arrayLength == null) {
            kw.Null(scripts);
        }
        else {
            scripts.add((writer, source) -> {
                int c = arrayLength.applyAsInt(source);
                if (c > 0) {
                    for (int i = 0; i < c - 1; i++) {
                        kw.Null(writer);
                        kw.NextArrayElement(writer, depth);
                    }
                    kw.Null(writer);
                }
            });
            kw.CloseArray(scripts, depth);
        }
    }

    void addBool(ToBoolFunction<T> func) {
        scripts.add((apendable, source) -> {
            if (func.applyAsBool(source)) {
                kw.True(apendable);
            }
            else {
                kw.False(apendable);
            }
        });
    }

    void addBool(ToNullableBoolFunction<T> func) {
        scripts.add((writer, source) -> func.applyAsBool(source, (b, isNull) -> {
                if (isNull) kw.Null(writer);
                else if (b) kw.True(writer);
                else kw.False(writer);
        }));
    }

    void addBool(ToBoolFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                addBool(func);
                break;
        }
    }

    void addBool(ToNullableBoolFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                addBool(func);
                break;
        }
    }

    // Integer

    void addInteger(ToLongFunction<T> func) {
        scripts.add((writer, source) -> Appendables.appendValue(writer, func.applyAsLong(source)));
    }

    void addInteger(IterLongFunction<T> func) {
        scripts.add((writer, source, i) -> {
            int count = arrayLength.applyAsInt(source);
            func.applyAsLong(source, i, v -> {
                if (i < count) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    Appendables.appendValue(writer, v);
                }
            });
            return i < count-1;
        });
        kw.CloseArray(scripts, depth);
    }

    void addInteger(ToNullableLongFunction<T> func) {
        scripts.add((writer, source) -> func.applyAsLong(source, (v, isNull) -> {
            if (isNull) {
                kw.Null(writer);
            }
            else {
                Appendables.appendValue(writer, v);
            }
        }));
    }

    void addInteger(IterNullableLongFunction<T> func) {
        scripts.add((writer, source, i) -> {
            int count = arrayLength.applyAsInt(source);
            func.applyAsLong(source, i, (v, isNull) -> {
                if (i < count) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    if (isNull) {
                        kw.Null(writer);
                    }
                    else {
                        Appendables.appendValue(writer, v);
                    }
                }
            });
            return i < count-1;
        });
        kw.CloseArray(scripts, depth);
    }

    void addInteger(ToLongFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                addInteger(func);
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    void addInteger(ToNullableLongFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                addInteger(func);
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    void addInteger(IterLongFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                addInteger(func);
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    void addInteger(IterNullableLongFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                addInteger(func);
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    // Decimal

    void addDecimal(ToDecimalFunction<T> func) {
        scripts.add((writer, source) -> func.applyAsDecimal(source, (m, e) ->
                Appendables.appendDecimalValue(writer, m, e)));
    }

    void addDecimal(ToNullableDecimalFunction<T> func) {
        scripts.add((writer, source) -> func.applyAsDecimal(source, (m, e, isNull) -> {
            if (isNull) {
                kw.Null(writer);
            }
            else {
                Appendables.appendDecimalValue(writer, m, e);
            }
        }));
    }

    void addDecimal(ToDecimalFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                addDecimal(func);
                break;
            case TypeBoolean:
                break;
        }
    }

    void addDecimal(ToNullableDecimalFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                addDecimal(func);
                break;
            case TypeBoolean:
                break;
        }
    }

    // String

    void addString(ToStringFunction<T> func) {
        kw.Quote(scripts);
        scripts.add((writer, source) -> writer.append(func.applyAsString(source)));
        kw.Quote(scripts);
    }

    void addnullableString(ToStringFunction<T> func) {
        scripts.add((writer, source) -> {
            CharSequence s = func.applyAsString(source);
            if (s == null) {
                kw.Null(writer);
            }
            else {
                kw.Quote(writer);
                writer.append(s);
                kw.Quote(writer);
            }
        });
    }

    void addString(ToStringFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                addString(func);
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    void addNullableString(ToStringFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                addnullableString(func);
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }
}
