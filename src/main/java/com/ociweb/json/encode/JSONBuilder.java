package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBranching;
import com.ociweb.json.template.StringTemplateBuilder;
import com.ociweb.json.appendable.AppendableByteWriter;
import com.ociweb.json.template.StringTemplateIterScript;
import com.ociweb.json.template.StringTemplateScript;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.util.Appendables;

// TODO: implement the primitive type converters
// TODO: refactor for duplicate code

class JSONBuilder<T> {
    private final StringTemplateBuilder<T> scripts;
    private final JSONKeywords kw;
    private final int depth;
    private final StringTemplateBuilder<T> objNullBranch;

    // Do not store mutable state used during render.
    // This is only used between begin and end object declarations.
    private int objectElementIndex = 0;

    JSONBuilder(StringTemplateBuilder<T> scripts, JSONKeywords kw, int depth) {
        this.scripts = scripts;
        this.kw = kw;
        this.depth = depth;
        objNullBranch = new StringTemplateBuilder<>();
        kw.Null(objNullBranch);
    }

    JSONKeywords getKeywords() {
        return kw;
    }

    void start() {
        kw.Start(scripts, depth);
    }

    void complete() {
        kw.Complete(scripts, depth);
        scripts.lock();
        objectElementIndex = -1;
    }

    boolean isLocked() {
        return scripts.isLocked();
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

    // Renderer

    // TODO: JSON builder should not be dependent on JSONRenderer
    // TODO: recursive renderers
    // TODO: selectable renderers

    <M> void addRenderer(final JSONRenderer<M> renderer, final ToMemberFunction<T, M> accessor) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                M member = accessor.get(source);
                if (member != null) {
                    renderer.render(writer, member);
                }
                else {
                    kw.Null(writer);
                }
            }
        });
    }

    <N, M> void addRenderer(final IteratorFunction<T, N> iterator, final JSONRenderer<M> renderer, final IterMemberFunction<T, M> accessor) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    M member = accessor.get(source, i);
                    if (member != null) {
                        renderer.render(writer, member);
                    }
                    else {
                        kw.Null(writer);
                    }
                }
                return node;
            }
        });
    }

    // Object

    public <M> StringTemplateBuilder<M> beginObject(final ToMemberFunction<T, M> accessor) {
        final StringTemplateBuilder<M> accessorScript = new StringTemplateBuilder<>();
        kw.OpenObj(accessorScript, depth);

        final StringTemplateBuilder<T> notNullBranch = new StringTemplateBuilder<>();
        notNullBranch.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                accessorScript.render(writer, accessor.get(source));
            }
        });

        final StringTemplateBuilder<T>[] nullableBranches = new StringTemplateBuilder[2];
        nullableBranches[0] = objNullBranch;
        nullableBranches[1] = notNullBranch;

        nullableBranches[1] = notNullBranch;
        scripts.add(nullableBranches, new StringTemplateBranching<T>() {
            @Override
            public int branch(T o) {
                return accessor.get(o) == null ? 0 : 1;
            }
        });
        return accessorScript;
    }

    <N, M> StringTemplateBuilder<M> beginObject(final IteratorFunction<T, N> iterator, final IterMemberFunction<T, M> accessor) {
        final StringTemplateBuilder<M> accessorBranch = new StringTemplateBuilder<>();
        kw.OpenObj(accessorBranch, depth);
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    M member = accessor.get(source, i);
                    if (member != null) {
                        accessorBranch.render(writer, member);
                    } else {
                        kw.Null(writer);
                    }
                }
                return node;
            }
        });
        return accessorBranch;
    }

    void endObject() {
        kw.CloseObj(scripts, depth);
    }

    // Array

    <M> StringTemplateBuilder<M> beginArray(final ToMemberFunction<T, M> func) {
        final StringTemplateBuilder<M> arrayBuilder = new StringTemplateBuilder<>();
        kw.OpenArray(arrayBuilder, depth);

        final StringTemplateBuilder<T> notNullBranch = new StringTemplateBuilder<>();
        notNullBranch.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                arrayBuilder.render(writer, func.get(source));
            }
        });

        final StringTemplateBuilder<T>[] nullableBranches = new StringTemplateBuilder[2];
        nullableBranches[0] = objNullBranch;
        nullableBranches[1] = notNullBranch;

        scripts.add(nullableBranches, new StringTemplateBranching<T>() {
            @Override
            public int branch(T o) {
                return func.get(o) == null ? 0 : 1;
            }
        });
        return arrayBuilder;
    }

    public <N, M> StringTemplateBuilder<M> beginArray(final IteratorFunction<T, N> iterator, final IterMemberFunction<T, M> func) {
        final StringTemplateBuilder<M> notNullBranch = new StringTemplateBuilder<>();
        kw.OpenArray(notNullBranch, depth);

        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    M element = func.get(source, i);
                    if (element == null) {
                        kw.Null(writer);
                    }
                    else {
                        notNullBranch.render(writer, element);
                    }
                }
                return node;
            }
        });
        return notNullBranch;
    }

    void endArray() {
        kw.CloseArray(scripts, depth);
    }

    // Null

    void addNull() {
        kw.Null(scripts);
    }

    <N> void addNull(final IteratorFunction<T, N> iterator) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                N node = null;
                for (int i = 0; (node = iterator.get(source, i, node)) != null; i++) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    kw.Null(writer);
                }
            }
        });
    }

    // Bool

    void addBool(final ToBoolFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                if (func.applyAsBool(source)) {
                    kw.True(writer);
                } else {
                    kw.False(writer);
                }
            }
        });
    }

    void addBool(final ToBoolFunction<T> isNull, final ToBoolFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(final AppendableByteWriter writer, T source) {
                if (isNull.applyAsBool(source)) {
                    kw.Null(writer);
                }
                else {
                    boolean b = func.applyAsBool(source);
                    if (b) {
                        kw.True(writer);
                    } else {
                        kw.False(writer);
                    }
                }
            }
        });
    }

    <N> void addBool(final IteratorFunction<T, N> iterator, final IterBoolFunction<T> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    boolean b = func.applyAsBool(source, i);
                    if (b) {
                        kw.True(writer);
                    } else {
                        kw.False(writer);
                    }
                }
                return node;
            }
        });
    }

    <N> void addBool(final IteratorFunction<T, N> iterator, final IterBoolFunction<T> isNull, final IterBoolFunction<T> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    if (isNull.applyAsBool(source, i)) {
                        kw.Null(writer);
                    }
                    else {
                        boolean b = func.applyAsBool(source, i);
                        if (b) {
                            kw.True(writer);
                        } else {
                            kw.False(writer);
                        }
                    }
                }
                return node;
            }
        });
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

    void addBool(final ToBoolFunction<T> isNull, final ToBoolFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                addBool(isNull, func);
                break;
        }
    }

    <N> void addBool(IteratorFunction<T, N> iterator, IterBoolFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                addBool(iterator, func);
                break;
        }
    }

    <N> void addBool(IteratorFunction<T, N> iterator, IterBoolFunction<T> isNull, IterBoolFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                addBool(iterator, isNull, func);
                break;
        }
    }

    // Integer

    void addInteger(final ToLongFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                Appendables.appendValue(writer, func.applyAsLong(source));
            }
        });
    }

    void addInteger(final ToBoolFunction<T> isNull, final ToLongFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                if (!isNull.applyAsBool(source)) {
                    Appendables.appendValue(writer, func.applyAsLong(source));
                }
                else {
                    kw.Null(writer);
                }
            }
        });
    }

    <N> void addInteger(final IteratorFunction<T, N> iterator, final IterLongFunction<T> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    long v = func.applyAsLong(source, i);
                    Appendables.appendValue(writer, v);
                }
                return node;
            }
        });
    }

    <N> void addInteger(final IteratorFunction<T, N> iterator, final IterBoolFunction<T> isNull, final IterLongFunction<T> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    if (isNull.applyAsBool(source, i)) {
                        kw.Null(writer);
                    }
                    else {
                        long v = func.applyAsLong(source, i);
                        Appendables.appendValue(writer, v);
                    }
                }
                return node;
            }
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

    void addInteger(ToBoolFunction<T> isNull, ToLongFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                addInteger(isNull, func);
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    <N> void addInteger(IteratorFunction<T, N> iterator, IterLongFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                addInteger(iterator, func);
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    <N> void addInteger(IteratorFunction<T, N> iterator, IterBoolFunction<T> isNull, IterLongFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                addInteger(iterator, isNull, func);
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    // Decimal

    // TODO: support rational, decimal

    void addDecimal(final int precision, final ToDoubleFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(final AppendableByteWriter writer, T source) {
                double value = func.applyAsDouble(source);
                Appendables.appendDecimalValue(writer, (long)(value * PipeWriter.powd[64 + precision]), (byte)(precision * -1));
            }
        });
    }

    void addDecimal(final int precision, final ToBoolFunction<T> isNull, final ToDoubleFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(final AppendableByteWriter writer, T source) {
                if (isNull.applyAsBool(source)) {
                    kw.Null(writer);
                }
                else {
                    double value = func.applyAsDouble(source);
                    Appendables.appendDecimalValue(writer, (long) (value * PipeWriter.powd[64 + precision]), (byte) (precision * -1));
                }
            }
        });
    }

    <N> void addDecimal(final IteratorFunction<T, N> iterator, final int precision, final IterDoubleFunction<T> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    double v = func.applyAsDouble(source, i);
                    Appendables.appendDecimalValue(writer, (long) (v * PipeWriter.powd[64 + precision]), (byte) (precision * -1));
                }
                return node;
            }
        });
    }

    <N> void addDecimal(final int precision, final IteratorFunction<T, N> iterator, final IterBoolFunction<T> isNull, final IterDoubleFunction<T> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    if (isNull.applyAsBool(source, i)) {
                        kw.Null(writer);
                    }
                    else {
                        double v = func.applyAsDouble(source, i);
                        Appendables.appendDecimalValue(writer, (long) (v * PipeWriter.powd[64 + precision]), (byte) (precision * -1));
                    }
                }
                return node;
            }
        });
    }

    void addDecimal(int precision, ToDoubleFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                addDecimal(precision, func);
                break;
            case TypeBoolean:
                break;
        }
    }

    void addDecimal(int precision, ToBoolFunction<T> isNull, ToDoubleFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                addDecimal(precision, isNull, func);
                break;
            case TypeBoolean:
                break;
        }
    }

    <N> void addDecimal(IteratorFunction<T, N> iterator, int precision, IterDoubleFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                addDecimal(iterator, precision, func);
                break;
            case TypeBoolean:
                break;
        }
    }

    <N> void addDecimal(int precision, IteratorFunction<T, N> iterator, IterBoolFunction<T> isNull, IterDoubleFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                addDecimal(precision, iterator, isNull, func);
                break;
            case TypeBoolean:
                break;
        }
    }

    // String

    // TODO: support Appendable writing directly writer

    void addString(final ToStringFunction<T> func) {
        kw.Quote(scripts);
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                writer.append(func.applyAsString(source));
            }
        });
        kw.Quote(scripts);
    }

    void addNullableString(final ToStringFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                CharSequence s = func.applyAsString(source);
                if (s == null) {
                    kw.Null(writer);
                } else {
                    kw.Quote(writer);
                    writer.append(s);
                    kw.Quote(writer);
                }
            }
        });
    }

    <N> void addString(final IteratorFunction<T, N> iterator, final IterStringFunction<T> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    CharSequence s = func.applyAsString(source, i);
                    kw.Quote(writer);
                    writer.append(s);
                    kw.Quote(writer);
                }
                return node;
            }
        });
    }

    <N> void addNullableString(final IteratorFunction<T, N> iterator, final IterStringFunction<T> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter writer, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    CharSequence s = func.applyAsString(source, i);
                    if (s == null) {
                        kw.Null(writer);
                    } else {
                        kw.Quote(writer);
                        writer.append(s);
                        kw.Quote(writer);
                    }
                }
                return node;
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

    <N> void addString(IteratorFunction<T, N> iterator, IterStringFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                addString(iterator, func);
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    <N> void addNullableString(IteratorFunction<T, N> iterator, IterStringFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                addNullableString(iterator, func);
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
