package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;
import com.ociweb.json.appendable.AppendableByteWriter;
import com.ociweb.json.template.StringTemplateIterScript;
import com.ociweb.pronghorn.util.Appendables;

class JSONBuilder<T> {
    private final StringTemplateBuilder<T> scripts;
    private final JSONKeywords kw;
    private final int depth;
    private final StringTemplateBuilder<T>[] nullableBranches = new StringTemplateBuilder[2];
    private int objectElementIndex = 0;
    // Make certain no mutable state is used during render!

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

    void start() {
        kw.Start(scripts, depth);
    }

    void complete() {
        kw.Complete(scripts, depth);
        if (nullableBranches[1] != null) nullableBranches[1].lock();
        scripts.lock();
        nullableBranches[0] = null;
        nullableBranches[1] = null;
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
        objectElementIndex++;
        scripts.add(name);
        kw.ObjectValue(scripts, depth);
        return this;
    }

    // Object

    StringTemplateBuilder<T>  beginObject() {
        kw.OpenObj(scripts, depth);
        return scripts;
    }

    StringTemplateBuilder<T> beginObject(final ToBoolFunction<T> isNull) {
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

    StringTemplateBuilder<T> beginArray(final ToBoolFunction<T> isNull) {
        StringTemplateBuilder<T> notNullBranch = new StringTemplateBuilder<>();
        kw.OpenArray(notNullBranch, depth);
        nullableBranches[1] = notNullBranch;
        scripts.add(nullableBranches, o -> isNull.applyAsBool(o) ? 0 : 1);
        return notNullBranch;
    }

    StringTemplateBuilder<T> endArray() {
        kw.CloseArray(scripts, depth);
        return scripts;
    }

    // Null

    void addNull() {
        kw.Null(scripts);
    }

    <I> void addNull(final ArrayIteratorFunction<T, I> iterator) {
        scripts.add((writer, source) -> {
            I node = null;
            for (int i = 0; (node = iterator.test(source, i, node)) != null; i++) {
                if (i > 0) {
                    kw.NextArrayElement(writer, depth);
                }
                kw.Null(writer);
            }
        });
    }

    // Bool

    void addBool(final ToBoolFunction<T> func) {
        scripts.add((apendable, source) -> {
            if (func.applyAsBool(source)) {
                kw.True(apendable);
            } else {
                kw.False(apendable);
            }
        });
    }

    void addBool(final ToNullableBoolFunction<T> func) {
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

    void addInteger(final ToLongFunction<T> func) {
        scripts.add((writer, source) -> Appendables.appendValue(writer, func.applyAsLong(source)));
    }

    <N> void addInteger(final ArrayIteratorFunction<T, N> iterator, final IterLongFunction<T, N> func) {
        scripts.add((StringTemplateIterScript<T, N>) (apendable, source, i, node) -> {
            node = iterator.test(source, i, node);
            if (node != null) {
                if (i > 0) {
                    kw.NextArrayElement(apendable, depth);
                }
                func.applyAsLong(source, i, node, v -> Appendables.appendValue(apendable, v));
            }
            return node;
        });
    }

    void addInteger(final ToNullableLongFunction<T> func) {
        scripts.add((writer, source) -> func.applyAsLong(source, (v, isNull) -> {
            if (isNull) {
                kw.Null(writer);
            } else {
                Appendables.appendValue(writer, v);
            }
        }));
    }

    <N> void addInteger(final ArrayIteratorFunction<T, N> iterator, final IterNullableLongFunction<T, N> func) {
        scripts.add((StringTemplateIterScript<T, N>) (appendable, source, i, node) -> {
            node = iterator.test(source, i, node);
            if (node != null) {
                if (i > 0) {
                    kw.NextArrayElement(appendable, depth);
                }
                func.applyAsLong(source, i, node, (v, isNull) -> {
                    if (isNull) {
                        kw.Null(appendable);
                    } else {
                        Appendables.appendValue(appendable, v);
                    }
                });
            }
            return node;
        });
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

    <N> void addInteger(ArrayIteratorFunction<T, N> arrayLength, IterLongFunction<T, N> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                addInteger(arrayLength, func);
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    <N> void addInteger(ArrayIteratorFunction<T, N> arrayLength, IterNullableLongFunction<T, N> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                addInteger(arrayLength, func);
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    // Decimal

    // TODO: support rational, decimal, and double

    void addDecimal(final ToDecimalFunction<T> func) {
        scripts.add((writer, source) -> func.applyAsDecimal(source, (m, e) -> Appendables.appendDecimalValue(writer, m, e)));
    }

    void addDecimal(final ToNullableDecimalFunction<T> func) {
        scripts.add((writer, source) -> func.applyAsDecimal(source, (m, e, isNull) -> {
            if (isNull) {
                kw.Null(writer);
            } else {
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

    // TODO: support Appendable writing directly writer

    void addString(final ToStringFunction<T> func) {
        kw.Quote(scripts);
        scripts.add((writer, source) -> writer.append(func.applyAsString(source)));
        kw.Quote(scripts);
    }

    void addNullableString(final ToStringFunction<T> func) {
        scripts.add((writer, source) -> {
            CharSequence s = func.applyAsString(source);
            if (s == null) {
                kw.Null(writer);
            } else {
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
                addNullableString(func);
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
