package com.ociweb.json.encode;

import com.ociweb.json.JSONType;
import com.ociweb.json.encode.function.IterBoolFunction;
import com.ociweb.json.encode.function.IterDoubleFunction;
import com.ociweb.json.encode.function.IterEnumFunction;
import com.ociweb.json.encode.function.IterLongFunction;
import com.ociweb.json.encode.function.IterMemberFunction;
import com.ociweb.json.encode.function.IterStringFunction;
import com.ociweb.json.encode.function.IteratorFunction;
import com.ociweb.json.encode.function.ToBoolFunction;
import com.ociweb.json.encode.function.ToDoubleFunction;
import com.ociweb.json.encode.function.ToEnumFunction;
import com.ociweb.json.encode.function.ToLongFunction;
import com.ociweb.json.encode.function.ToMemberFunction;
import com.ociweb.json.encode.function.ToStringFunction;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.util.AppendableByteWriter;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ByteWriter;
import com.ociweb.pronghorn.util.template.StringTemplateBranching;
import com.ociweb.pronghorn.util.template.StringTemplateBuilder;
import com.ociweb.pronghorn.util.template.StringTemplateIterScript;
import com.ociweb.pronghorn.util.template.StringTemplateScript;

// TODO: support rational, decimal
// TODO: implement the primitive type converters, or refactor to use lambdas, or delete methods
// TODO: refactor for duplicate code
// TODO: determine use case for recursive array and either uncomment/test or delete methods
// TODO: implement select as an array element

// Maintain no dependencies to the public API classes (i.e. JSONObject)

class JSONBuilder<R, T> implements StringTemplateScript<T> {
    // Do not store mutable state used during render.
    // Use ThreadLocal if required.
    private final StringTemplateBuilder<T> scripts;
    private final JSONKeywords kw;
    private final int depth;
    private JSONBuilder<R, R> root;

    // Stored between declaration calls and consumed on use in declaration
    private byte[] declaredMemberName;

    // In order to support tryCase, we need a render state for objects.
    private ObjectRenderState ors;

    JSONBuilder() {
        this(new StringTemplateBuilder<T>(), JSONKeywords.instance, 0, null);
    }

    JSONBuilder(JSONKeywords kw) {
        this(new StringTemplateBuilder<T>(), kw, 0, null);
    }

    private JSONBuilder(StringTemplateBuilder<T> scripts, JSONKeywords kw, int depth, JSONBuilder<R, R> root) {
        this.scripts = scripts;
        this.kw = kw;
        this.depth = depth;
        this.root = root;

        if (root == null) {
            this.root = (JSONBuilder<R, R>)this;
        }
    }

    void start() {
        kw.Start(scripts, depth);
    }

    void complete() {
        kw.Complete(scripts, depth);
    }

    public void render(AppendableByteWriter writer, T source) {
        scripts.render(writer, source);
    }

    // Object Helpers

    JSONBuilder<R, T> addFieldPrefix(String name) {
        this.declaredMemberName = name.getBytes();
        return this;
    }

    private static class ObjectRenderState {
        private final JSONKeywords kw;
        private int objectElementIndex = -1;

        ObjectRenderState(JSONKeywords kw) {
            this.kw = kw;
        }

        private void beginObjectRender() {
            objectElementIndex = -1;
        }

        void prefixObjectMemberName(byte[] declaredMemberName, int depth, ByteWriter writer) {
            objectElementIndex++;
            if (objectElementIndex == 0) {
                kw.FirstObjectElement(writer, depth);
            }
            else {
                kw.NextObjectElement(writer, depth);
            }
            writer.write(declaredMemberName);
            kw.ObjectValue(writer, depth);
        }
    }

    private void prefixObjectMemberName(byte[] declaredMemberName, int depth, ByteWriter writer) {
        if (declaredMemberName != null && this.ors != null) {
            ObjectRenderState ors = this.ors;
            if (ors != null) {
                ors.prefixObjectMemberName(declaredMemberName, depth, writer);
            }
        }
    }

    private StringTemplateScript<T> createNullObjectScript(final byte[] declaredMemberName) {
        return new StringTemplateScript<T>() {
            @Override
            public void render(AppendableByteWriter writer, T source) {
                prefixObjectMemberName(declaredMemberName, depth, writer);
                kw.Null(writer);
            }
        };
    }

    private byte[] consumeDeclaredMemberName() {
        byte[] declaredMemberName = this.declaredMemberName;
        this.declaredMemberName = null;
        return declaredMemberName;
    }

    // Array Helpers

    interface RenderIteration<M, N> {
        void render(AppendableByteWriter appendable, M member, int i);
    }

    private <M, N> void iterate(
            final IteratorFunction<T, N> iterator,
            final boolean checkNull,
            final IterMemberFunction<T, M> accessor,
            final RenderIteration<M, N> func) {
        scripts.add(new StringTemplateIterScript<T>() {
            @Override
            public boolean render(final AppendableByteWriter writer, T source, int i) {
                N node = iterator.get(source, i);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    M member = accessor.get(source, i);
                    if (checkNull && member == null) {
                        kw.Null(writer);
                    } else {
                        func.render(writer, member, i);
                    }
                    return true;
                }
                return false;
            }
        });
    }

    private <N> void iterate(
            final IteratorFunction<T, N> iterator,
            final IterBoolFunction<T> isNull,
            final RenderIteration<T, N> func) {
        scripts.add(new StringTemplateIterScript<T>() {
            @Override
            public boolean render(final AppendableByteWriter writer, T source, int i) {
                N node = iterator.get(source, i);
                if (node != null) {
                    if (i > 0) {
                        kw.NextArrayElement(writer, depth);
                    }
                    if (isNull != null && isNull.applyAsBool(source, i)) {
                        kw.Null(writer);
                    } else {
                        func.render(writer, source, i);
                    }
                    return true;
                }
                return false;
            }
        });
    }

    // Sub Builders

    <M> void addBuilder(final JSONBuilder<?, M> builder, final ToMemberFunction<T, M> accessor) {
        final byte[] declaredMemberName = consumeDeclaredMemberName();
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void render(AppendableByteWriter writer, T source) {
                prefixObjectMemberName(declaredMemberName, depth, writer);
                M member = accessor.get(source);
                if (member == null) {
                    kw.Null(writer);
                }
                else {
                    builder.render(writer, member);
                }
            }
        });
    }

    void recurseRoot(final ToMemberFunction<T, R> accessor) {
        if (root != this) {
            addBuilder(root, accessor);
        }
    }

    <N, M> void addBuilder(final IteratorFunction<T, N> iterator, final JSONBuilder<?, M> builder, final IterMemberFunction<T, M> accessor) {
        iterate(iterator, true, accessor, new RenderIteration<M, N>() {
            @Override
            public void render(AppendableByteWriter writer, M m, int i) {
                builder.render(writer, m);
            }
        });
    }

/*
    <N> void recurseRoot(final IteratorFunction<T, N> iterator, final IterMemberFunction<T, R> accessor) {
        addBuilder(iterator, root, accessor);
    }
*/
    // Select

    JSONBuilder<R, T> beginSelect() {
        final byte[] declaredMemberName = consumeDeclaredMemberName();
        JSONBuilder<R, T> builder = new JSONBuilder<R, T>(new StringTemplateBuilder<T>(), kw, depth, root);
        builder.declaredMemberName = declaredMemberName;
        builder.ors = ors;
        this.scripts.add(builder);
        return builder;
    }

    JSONBuilder<R, T> tryCase() {
        final byte[] declaredMemberName = this.declaredMemberName; // Do not consume for other try
        JSONBuilder<R, T> builder = new JSONBuilder<R, T>(new StringTemplateBuilder<T>(), kw, depth, root);
        builder.declaredMemberName = declaredMemberName;
        builder.ors = ors;
        return builder;
    }

    void endSelect(final int count, final ToBoolFunction<T>[] branches, final JSONBuilder<?, T>[] cases) {
        consumeDeclaredMemberName();
        final StringTemplateScript<T>[] caseScripts = new StringTemplateScript[count];
        for (int i = 0; i < count; i++) {
            final JSONBuilder<?, T> builder = cases[i];
            caseScripts[i] = builder;
        }

        scripts.add(caseScripts, new StringTemplateBranching<T>() {
            @Override
            public int branch(T source) {
                for (int i = 0; i < count; i++) {
                    if (branches[i].applyAsBool(source)) {
                        return i;
                    }
                }
                return -1;
            }
        });
    }

    <N> void endSelect(IteratorFunction<T, N> iterator, int count, final IterBoolFunction<T>[] branches, final JSONBuilder<?, T>[] cases) {
    }

    // Object

    public <M> JSONBuilder<R, M> beginObject(final ToMemberFunction<T, M> accessor) {
        final byte[] declaredMemberName = consumeDeclaredMemberName();
        final StringTemplateBuilder<M> accessorScript = new StringTemplateBuilder<>();
        kw.OpenObj(accessorScript, depth);

        final ObjectRenderState newOrs = new ObjectRenderState(kw);

        final StringTemplateScript<T> objNullBranch = createNullObjectScript(declaredMemberName);
        final StringTemplateScript<T> notNullBranch = new StringTemplateScript<T>() {
            @Override
            public void render(AppendableByteWriter writer, T source) {
                prefixObjectMemberName(declaredMemberName, depth, writer);
                newOrs.beginObjectRender();
                accessorScript.render(writer, accessor.get(source));
            }
        };

        final StringTemplateScript<T>[] nullableBranches = new StringTemplateScript[2];
        nullableBranches[0] = objNullBranch;
        nullableBranches[1] = notNullBranch;

        scripts.add(nullableBranches, new StringTemplateBranching<T>() {
            @Override
            public int branch(T o) {
                return accessor.get(o) == null ? 0 : 1;
            }
        });
        JSONBuilder<R, M> builder = new JSONBuilder<>(accessorScript, kw, depth + 1, root);
        builder.ors = newOrs;
        return builder;
    }

    <N, M> JSONBuilder<R, M> beginObject(final IteratorFunction<T, N> iterator, final IterMemberFunction<T, M> accessor) {
        final StringTemplateBuilder<M> accessorBranch = new StringTemplateBuilder<>();
        kw.OpenObj(accessorBranch, depth);

        final ObjectRenderState newOrs = new ObjectRenderState(kw);

        iterate(iterator, true, new IterMemberFunction<T, M>() {
            @Override
            public M get(T o, int i) {
                newOrs.beginObjectRender();
                return accessor.get(o, i);
            }
        }, new RenderIteration<M, N>() {
            @Override
            public void render(AppendableByteWriter writer, M m, int i) {
                accessorBranch.render(writer, m);
            }
        });

        JSONBuilder<R, M> builder = new JSONBuilder<>(accessorBranch, kw, depth + 1, root);
        builder.ors = newOrs;
        return builder;
    }

    void endObject() {
        kw.CloseObj(scripts, depth);
    }

    // Array

    <M> JSONBuilder<R, M> beginArray(final ToMemberFunction<T, M> func) {
        final byte[] declaredMemberName = consumeDeclaredMemberName();
        final StringTemplateBuilder<M> arrayBuilder = new StringTemplateBuilder<>();
        kw.OpenArray(arrayBuilder, depth);

        final StringTemplateScript<T> objNullBranch = createNullObjectScript(declaredMemberName);
        final StringTemplateScript<T> notNullBranch = new StringTemplateScript<T>() {
            @Override
            public void render(AppendableByteWriter writer, T source) {
                prefixObjectMemberName(declaredMemberName, depth, writer);
                arrayBuilder.render(writer, func.get(source));
            }
        };

        final StringTemplateScript<T>[] nullableBranches = new StringTemplateScript[2];
        nullableBranches[0] = objNullBranch;
        nullableBranches[1] = notNullBranch;

        scripts.add(nullableBranches, new StringTemplateBranching<T>() {
            @Override
            public int branch(T o) {
                return func.get(o) == null ? 0 : 1;
            }
        });
        return new JSONBuilder<R, M>(arrayBuilder, kw, depth + 1, root);
    }

    public <N, M> JSONBuilder<R, M> beginArray(final IteratorFunction<T, N> iterator, final IterMemberFunction<T, M> func) {
        final StringTemplateBuilder<M> notNullBranch = new StringTemplateBuilder<>();
        kw.OpenArray(notNullBranch, depth);

        iterate(iterator, true, func, new RenderIteration<M, N>() {
            @Override
            public void render(AppendableByteWriter writer, M m, int i) {
                notNullBranch.render(writer, m);
            }
        });
        return new JSONBuilder<R, M>(notNullBranch, kw, depth + 1, root);
    }

    void endArray() {
        kw.CloseArray(scripts, depth);
    }

    // Null

    void addNull() {
        final byte[] declaredMemberName = consumeDeclaredMemberName();
        scripts.add(createNullObjectScript(declaredMemberName));
    }

    <N> void addNull(final IteratorFunction<T, N> iterator) {
        iterate(iterator, new IterBoolFunction<T>() {
            @Override
            public boolean applyAsBool(T o, int i) {
                return true;
            }
        }, null);
    }

    // Bool

    void addBool(final ToBoolFunction<T> isNull, final ToBoolFunction<T> func) {
        final byte[] declaredMemberName = consumeDeclaredMemberName();
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void render(final AppendableByteWriter writer, T source) {
                prefixObjectMemberName(declaredMemberName, depth, writer);
                if (isNull != null && isNull.applyAsBool(source)) {
                    kw.Null(writer);
                }
                else {
                    if (func.applyAsBool(source)) {
                        kw.True(writer);
                    }
                    else {
                        kw.False(writer);
                    }
                }
            }
        });
    }

    <N> void addBool(final IteratorFunction<T, N> iterator, final IterBoolFunction<T> isNull, final IterBoolFunction<T> func) {
        iterate(iterator, isNull, new RenderIteration<T, N>() {
            @Override
            public void render(AppendableByteWriter writer, T source, int i) {
                if (func.applyAsBool(source, i)) {
                    kw.True(writer);
                }
                else {
                    kw.False(writer);
                }
            }});
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

    void addInteger(final ToBoolFunction<T> isNull, final ToLongFunction<T> func) {
        final byte[] declaredMemberName = consumeDeclaredMemberName();
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void render(AppendableByteWriter writer, T source) {
                prefixObjectMemberName(declaredMemberName, depth, writer);
                if (isNull != null && isNull.applyAsBool(source)) {
                    kw.Null(writer);
                }
                else {
                    Appendables.appendValue(writer, func.applyAsLong(source), false);
                }
            }
        });
    }

    <N> void addInteger(final IteratorFunction<T, N> iterator, final IterBoolFunction<T> isNull, final IterLongFunction<T> func) {
        iterate(iterator, isNull, new RenderIteration<T, N>() {
            @Override
            public void render(AppendableByteWriter writer, T source, int i) {
                Appendables.appendValue(writer, func.applyAsLong(source, i), false);
            }});
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

    void addDecimal(final int precision, final ToBoolFunction<T> isNull, final ToDoubleFunction<T> func) {
        final byte[] declaredMemberName = consumeDeclaredMemberName();
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void render(final AppendableByteWriter writer, T source) {
                prefixObjectMemberName(declaredMemberName, depth, writer);
                if (isNull != null && isNull.applyAsBool(source)) {
                    kw.Null(writer);
                }
                else {
                    double v = func.applyAsDouble(source);
                    Appendables.appendDecimalValue(writer, (long) (v * PipeWriter.powd[64 + precision]), (byte) (precision * -1));
                }
            }
        });
    }

    <N> void addDecimal(final IteratorFunction<T, N> iterator, final int precision, final IterBoolFunction<T> isNull, final IterDoubleFunction<T> func) {
        iterate(iterator, isNull, new RenderIteration<T, N>() {
            @Override
            public void render(AppendableByteWriter writer, T source, int i) {
                double v = func.applyAsDouble(source, i);
                Appendables.appendDecimalValue(writer, (long) (v * PipeWriter.powd[64 + precision]), (byte) (precision * -1));
            }
        });
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

    <N> void addDecimal(IteratorFunction<T, N> iterator, int precision, IterBoolFunction<T> isNull, IterDoubleFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                addDecimal(iterator, precision, isNull, func);
                break;
            case TypeBoolean:
                break;
        }
    }

    // String

    void addString(final boolean checkNull, final ToStringFunction<T> func) {
        final byte[] declaredMemberName = consumeDeclaredMemberName();
        
        scripts.add(new StringTemplateScript<T>() {
        	
        	NullableAppendableByteWriterWrapper nabww = new NullableAppendableByteWriterWrapper();
        	
            @Override
            public void render(AppendableByteWriter writer, T source) {
                prefixObjectMemberName(declaredMemberName, depth, writer);
                
                if (!checkNull) {
                	kw.Quote(writer);
                	func.applyAsString(source, writer);
                	kw.Quote(writer);                	
                } else {
                	nabww.externalWriter = writer;
                	nabww.wasNull = false;
                	nabww.needsQuote = true;
                	func.applyAsString(source, nabww);
                	if (!nabww.wasNull) {
                		kw.Quote(writer);
                	} else {
                		//Note we are already reset to the beginning.
                		kw.Null(writer);
                	}
                }
                
            }
        });
    }

    <N> void addString(final IteratorFunction<T, N> iterator,final boolean checkNull, final IterStringFunction<T> func) {
        iterate(iterator, checkNull, new IterMemberFunction<T, T>() {
            @Override
            public T get(T o, int i) {
            	return o;                
            }
        }, new RenderIteration<T, N>() {
        	
        	NullableAppendableByteWriterWrapper nabww = new NullableAppendableByteWriterWrapper();

            @Override
            public void render(AppendableByteWriter writer, T o, int i) {

                if (!checkNull) {
                	kw.Quote(writer);
                	func.applyAsString(o, i, writer);
                	kw.Quote(writer);                	
                } else {
                	nabww.externalWriter = writer;
                	nabww.wasNull = false;
                	nabww.needsQuote = true;
                	func.applyAsString(o, i, nabww);
                	if (!nabww.wasNull) {
                		kw.Quote(writer);
                	} else {
                		//Note we are already reset to the beginning.
                		kw.Null(writer);
                	}
                }
            }
        });
    }

    void addString(boolean checkNull, ToStringFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                addString(checkNull, func);
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    <N> void addString(IteratorFunction<T, N> iterator, boolean checkNull, IterStringFunction<T> func, JSONType encode) {
        switch (encode) {
            case TypeString:
                addString(iterator, checkNull, func);
                break;
            case TypeInteger:
                break;
            case TypeDecimal:
                break;
            case TypeBoolean:
                break;
        }
    }

    // Enum

    <E extends Enum<E>> void addEnumName(final ToEnumFunction<T, E> func) {
        addString(true, new ToStringFunction<T>() {

			@Override
			public void applyAsString(T value, AppendableByteWriter<?> target) {
                E v = func.applyAsEnum(value);
                if (null!=v) {
                	target.append(v.name());
                } else {
                	target.append(null);
                }
			}
        });
    }

    <N, E extends Enum<E>> void addEnumName(final IteratorFunction<T, N> iterator, final IterEnumFunction<T, E> func) {
        iterate(iterator, true, new IterMemberFunction<T, CharSequence>() {
            @Override
            public CharSequence get(T o, int i) {
                E v = func.applyAsEnum(o, i);
                return (v != null ? v.name() : null);
            }
        }, new RenderIteration<CharSequence, N>() {
            @Override
            public void render(AppendableByteWriter writer, CharSequence member, int i) {
                kw.Quote(writer);
                writer.append(member);
                kw.Quote(writer);
            }
        });
    }

    <E extends Enum<E>> void addEnumOrdinal(final ToEnumFunction<T, E> func) {
        final byte[] declaredMemberName = consumeDeclaredMemberName();
        scripts.add(new StringTemplateScript<T>() {
            @Override
            public void render(AppendableByteWriter writer, T source) {
                prefixObjectMemberName(declaredMemberName, depth, writer);
                E v = func.applyAsEnum(source);
                if (v == null) {
                    kw.Null(writer);
                }
                else {
                    Appendables.appendValue(writer, v.ordinal());
                }
            }
        });
    }

    <N, E extends Enum<E>> void addEnumOrdinal(final IteratorFunction<T, N> iterator, final IterEnumFunction<T, E> func) {
        iterate(iterator, true, new IterMemberFunction<T, Enum>() {
            @Override
            public Enum get(T o, int i) {
                return func.applyAsEnum(o, i);
            }
        }, new RenderIteration<Enum, N>() {
            @Override
            public void render(AppendableByteWriter writer, Enum member, int i) {
                Appendables.appendValue(writer, member.ordinal());
            }
        });
    }
}
