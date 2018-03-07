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

    <M> void addRenderer(final JSONRenderer<M> renderer, final ToMemberFunction<T, M> accessor) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter appendable, T source) {
                M member = accessor.get(source);
                if (member != null) {
                    renderer.render(appendable, member);
                }
                else {
                    kw.Null(appendable);
                }
            }
        });
    }

    <N, M> void addRenderer(final IterMemberFunction<T, N, N> iterator, final JSONRenderer<M> renderer, final IterMemberFunction<T, N, M> accessor) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(appendable, depth);
                    }
                    M member = accessor.get(source, i, node);
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

    public <M> StringTemplateBuilder<M> beginObject(ToMemberFunction<T, M> accessor) {
        StringTemplateBuilder<M> accessorScript = new StringTemplateBuilder<>();
        kw.OpenObj(accessorScript, depth);

        StringTemplateBuilder<T> notNullBranch = new StringTemplateBuilder<>();
        notNullBranch.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter appendable, T source) {
                accessorScript.render(appendable, accessor.get(source));
            }
        });

        StringTemplateBuilder<T>[] nullableBranches = new StringTemplateBuilder[2];
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

    <N, M> StringTemplateBuilder<M> beginObject(final IterMemberFunction<T, N, N> iterator, final IterMemberFunction<T, N, M> accessor) {
        StringTemplateBuilder<M> accessorBranch = new StringTemplateBuilder<>();
        kw.OpenObj(accessorBranch, depth);
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.get(source, i, node);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(appendable, depth);
                    }
                    M member = accessor.get(source, i, node);
                    if (member != null) {
                        accessorBranch.render(appendable, member);
                    } else {
                        kw.Null(appendable);
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

    StringTemplateBuilder<T> beginArray() {
        kw.OpenArray(scripts, depth);
        return scripts;
    }

    StringTemplateBuilder<T> beginArray(final ToBoolFunction<T> isNull) {
        StringTemplateBuilder<T> notNullBranch = new StringTemplateBuilder<>();
        kw.OpenArray(notNullBranch, depth);

        StringTemplateBuilder[] nullableBranches = new StringTemplateBuilder[2];
        nullableBranches[0] = objNullBranch;
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

    <N> void addNull(final IterMemberFunction<T, N, N> iterator) {
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

    <N> void addBool(IterMemberFunction<T, N, N> iterator, IterBoolFunction<T, N> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.get(source, i, node);
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

    <N> void addBool(IterMemberFunction<T, N, N> iterator, IterBoolFunction<T, N> func, JSONType encode) {
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

    // Integer

    void addInteger(final ToLongFunction<T> func) {
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void fetch(AppendableByteWriter writer, T source) {
                Appendables.appendValue(writer, func.applyAsLong(source));
            }
        });
    }

    <N> void addInteger(final IterMemberFunction<T, N, N> iterator, final IterLongFunction<T, N> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.get(source, i, node);
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
    <N> void addInteger(final IterMemberFunction<T, N, N> iterator, final IterNullableLongFunction<T, N> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.get(source, i, node);
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

    <N> void addInteger(IterMemberFunction<T, N, N> arrayLength, IterLongFunction<T, N> func, JSONType encode) {
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
    <N> void addInteger(IterMemberFunction<T, N, N> arrayLength, IterNullableLongFunction<T, N> func, JSONType encode) {
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

    <N> void addDecimal(IterMemberFunction<T, N, N> iterator, int precision, IterDoubleFunction<T, N> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.get(source, i, node);
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

    <N> void addDecimal(IterMemberFunction<T, N, N> iterator, int precision, IterDoubleFunction<T, N> func, JSONType encode) {
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

    <N> void addString(IterMemberFunction<T, N, N> iterator, IterStringFunction<T, N> func) {
        scripts.add(new StringTemplateIterScript<T, N>() {
            @Override
            public N fetch(final AppendableByteWriter appendable, T source, int i, N node) {
                node = iterator.get(source, i, node);
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

    <N> void addString(IterMemberFunction<T, N, N> iterator, IterStringFunction<T, N> func, JSONType encode) {
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
