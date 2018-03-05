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

// TODO: implement the type converters

class JSONBuilder<T> {
    private final StringTemplateBuilder<T> scripts;
    private final JSONKeywords kw;
    private final int depth;

    // Do not store mutable state used during render. nullableBranches becomes null
    private int objectElementIndex = 0;
    private final StringTemplateBuilder<T>[] nullableBranches = new StringTemplateBuilder[2];

    JSONBuilder(StringTemplateBuilder<T> scripts, JSONKeywords kw, int depth) {
        this.scripts = scripts;
        this.kw = kw;
        this.depth = depth;
        StringTemplateBuilder<T> objNullBranch = new StringTemplateBuilder<>();
        kw.Null(objNullBranch);
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
        scripts.lock();
        nullableBranches[0] = null;
        nullableBranches[1] = null;
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

    <M> void addRenderer(final JSONRenderer<M> renderer, final ToMemberFunction<T, M> accessor) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter appendable, T source) {
                M member = accessor.apply(source);
                if (member != null) {
                    renderer.render(appendable, member);
                }
                else {
                    kw.Null(appendable);
                }
            }
        });
    }

    <N, M> void addRenderer(final ArrayIteratorFunction<T, N> iterator, final JSONRenderer<M> renderer, final IterMemberFunction<T, N, M> accessor) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.test(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(appendable, depth);
                    }
                    M member = accessor.apply(source, i, node);
                    if (member != null) {
                        renderer.render(appendable, member);
                    }
                    else {
                        kw.Null(appendable);
                    }
                }
                return node;
            }
        });
    }

    // Object

    StringTemplateBuilder<T>  beginObject() {
        kw.OpenObj(scripts, depth);
        return scripts;
    }

    <M> StringTemplateBuilder<M> beginObject(final ToMemberFunction<T, M> accessor) {
        StringTemplateBuilder<M> accessorBranch = new StringTemplateBuilder<>();
        kw.OpenObj(accessorBranch, depth);
        scripts.add(accessorBranch, accessor);
        return accessorBranch;
    }

    StringTemplateBuilder<T> beginObject(final ToBoolFunction<T> isNull) {
        StringTemplateBuilder<T> notNullBranch = new StringTemplateBuilder<>();
        kw.OpenObj(notNullBranch, depth);
        nullableBranches[1] = notNullBranch;
        scripts.add(nullableBranches, new StringTemplateBranching<T>() {
            @Override
            public int branch(T o) {
                return isNull.applyAsBool(o) ? 0 : 1;
            }
        });
        return notNullBranch;
    }

    <N, M> StringTemplateBuilder<M> beginObject(final ArrayIteratorFunction<T, N> iterator, final IterMemberFunction<T, N, M> accessor) {
        StringTemplateBuilder<M> accessorBranch = new StringTemplateBuilder<>();
        kw.OpenObj(accessorBranch, depth);
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.test(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(appendable, depth);
                    }
                    M member = accessor.apply(source, i, node);
                    if (member != null) {
                        accessorBranch.render(appendable, member);
                    } else {
                        kw.Null(appendable);
                    }
                    kw.CloseObj(appendable, depth);
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

    StringTemplateBuilder<T> beginArray() {
        kw.OpenArray(scripts, depth);
        return scripts;
    }

    StringTemplateBuilder<T> beginArray(final ToBoolFunction<T> isNull) {
        StringTemplateBuilder<T> notNullBranch = new StringTemplateBuilder<>();
        kw.OpenArray(notNullBranch, depth);
        nullableBranches[1] = notNullBranch;
        scripts.add(nullableBranches, new StringTemplateBranching<T>() {
            @Override
            public int branch(T o) {
                return isNull.applyAsBool(o) ? 0 : 1;
            }
        });
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
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                I node = null;
                for (int i = 0; (node = iterator.test(source, i, node)) != null; i++) {
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
            public void fetch(AppendableByteWriter appendable, T source) {
                if (func.applyAsBool(source)) {
                    kw.True(appendable);
                } else {
                    kw.False(appendable);
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

    <N> void addBool(ArrayIteratorFunction<T, N> iterator, IterBoolFunction<T, N> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.test(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(appendable, depth);
                    }
                    func.applyAsBool(source, i, node, new IterBoolFunction.Visit() {
                        @Override
                        public void visit(boolean b) {
                            if (b) {
                                kw.True(appendable);
                            } else {
                                kw.False(appendable);
                            }
                        }
                    });
                }
                return node;
            }
        });
    }

    @Deprecated
    void addBool(final ToNullableBoolFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(final AppendableByteWriter writer, T source) {
                func.applyAsBool(source, new ToNullableBoolFunction.Visit() {
                    @Override
                    public void visit(boolean b, boolean isNull) {
                        if (isNull) kw.Null(writer);
                        else if (b) kw.True(writer);
                        else kw.False(writer);
                    }
                });
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

    <N> void addBool(ArrayIteratorFunction<T, N> iterator, IterBoolFunction<T, N> func, JSONType encode) {
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

    @Deprecated
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
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                Appendables.appendValue(writer, func.applyAsLong(source));
            }
        });
    }

    <N> void addInteger(final ArrayIteratorFunction<T, N> iterator, final IterLongFunction<T, N> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.test(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(appendable, depth);
                    }
                    func.applyAsLong(source, i, node, new IterLongFunction.Visit() {
                        @Override
                        public void visit(long v) {
                            Appendables.appendValue(appendable, v);
                        }
                    });
                }
                return node;
            }
        });
    }

    void addInteger(ToBoolFunction<T> isNull, ToLongFunction<T> func) {
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

    @Deprecated
    void addInteger(final ToNullableLongFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(final AppendableByteWriter writer, T source) {
                func.applyAsLong(source, new ToNullableLongFunction.Visit() {
                    @Override
                    public void visit(long v, boolean isNull) {
                        if (isNull) {
                            kw.Null(writer);
                        } else {
                            Appendables.appendValue(writer, v);
                        }
                    }
                });
            }
        });
    }

    @Deprecated
    <N> void addInteger(final ArrayIteratorFunction<T, N> iterator, final IterNullableLongFunction<T, N> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.test(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(appendable, depth);
                    }
                    func.applyAsLong(source, i, node, new IterNullableLongFunction.Visit() {
                        @Override
                        public void visit(long v, boolean isNull) {
                            if (isNull) {
                                kw.Null(appendable);
                            } else {
                                Appendables.appendValue(appendable, v);
                            }
                        }
                    });
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

    @Deprecated
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

    @Deprecated
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

    // TODO: support rational, decimal

    void addDecimal(int precision, ToDoubleFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(final AppendableByteWriter writer, T source) {
                double value = func.applyAsDouble(source);
                Appendables.appendDecimalValue(writer, (long)(value * PipeWriter.powd[64 + precision]), (byte)(precision * -1));
            }
        });
    }

    void addDecimal(int precision, final ToBoolFunction<T> isNull, ToDoubleFunction<T> func) {
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

    <N> void addDecimal(ArrayIteratorFunction<T, N> iterator, int precision, IterDoubleFunction<T, N> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.test(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(appendable, depth);
                    }
                    func.applyAsDouble(source, i, node, new IterDoubleFunction.Visit() {
                        @Override
                        public void visit(double v) {
                            Appendables.appendDecimalValue(appendable, (long) (v * PipeWriter.powd[64 + precision]), (byte) (precision * -1));
                        }
                    });
                }
                return node;
            }
        });
    }

    @Deprecated
    void addDecimal(final ToDecimalFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(final AppendableByteWriter writer, T source) {
                func.applyAsDecimal(source, new ToDecimalFunction.Visit() {
                    @Override
                    public void visit(double value, int precision) {
                        Appendables.appendDecimalValue(writer, (long)(value * PipeWriter.powd[64 + precision]), (byte)(precision * -1));
                    }
                });
            }
        });
    }

    @Deprecated
    void addDecimal(final ToBoolFunction<T> isNull, final ToDecimalFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(final AppendableByteWriter writer, T source) {
                func.applyAsDecimal(source, new ToDecimalFunction.Visit() {
                    @Override
                    public void visit(double value, int precision) {
                        if (isNull.applyAsBool(source)) {
                            kw.Null(writer);
                        } else {
                            Appendables.appendDecimalValue(writer, (long)(value * PipeWriter.powd[64 + precision]), (byte)(precision * -1));
                        }
                    }
                });
            }
        });
    }

    @Deprecated
    void addDecimal(final ToNullableDecimalFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(final AppendableByteWriter writer, T source) {
                func.applyAsDecimal(source, new ToNullableDecimalFunction.Visit() {
                    @Override
                    public void visit(double value, int precision, boolean isNull) {
                        if (isNull) {
                            kw.Null(writer);
                        } else {
                            Appendables.appendDecimalValue(writer, (long)(value * PipeWriter.powd[64 + precision]), (byte)(precision * -1));
                        }
                    }
                });
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

    <N> void addDecimal(ArrayIteratorFunction<T, N> iterator, int precision, IterDoubleFunction<T, N> func, JSONType encode) {
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

    @Deprecated
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

    @Deprecated
    void addDecimal(final ToBoolFunction<T> isNull, ToDecimalFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                addDecimal(isNull, func);
                break;
            case TypeBoolean:
                break;
        }
    }

    @Deprecated
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

    <N> void addString(ArrayIteratorFunction<T, N> iterator, IterStringFunction<T, N> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.test(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(appendable, depth);
                    }
                    func.applyAsString(source, i, node, new IterStringFunction.Visit() {
                        @Override
                        public void visit(String v) {
                            kw.Quote(appendable);
                            appendable.append(v);
                            kw.Quote(appendable);
                        }
                    });
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

    <N> void addString(ArrayIteratorFunction<T, N> iterator, IterStringFunction<T, N> func, JSONType encode) {
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
}
