package com.ociweb.pronghorn.structure.processor;

import com.ociweb.pronghorn.structure.annotations.ProngStruct;
import com.ociweb.pronghorn.structure.processor.property.pojo.PojoPropertyBuilderFactory;
import com.ociweb.pronghorn.structure.processor.property.readonly.ReadonlyPropertyBuilderFactory;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Set;

@SupportedAnnotationTypes({"com.ociweb.pronghorn.structure.*"})
@SupportedSourceVersion(SourceVersion.RELEASE_7)
public class ProngStructProcessor extends AbstractProcessor {
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (annotations.isEmpty()) {
            return true;
        }
        try {
            for (Element element : roundEnv.getElementsAnnotatedWith(ProngStruct.class)) {
                if (element.getKind() == ElementKind.INTERFACE || (element.getKind() == ElementKind.CLASS && element.getModifiers().contains(Modifier.ABSTRACT))) {
                    ProngStruct annotation = element.getAnnotation(ProngStruct.class);
                    if (annotation.suffix().isEmpty()) {
                        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, element.toString() + " cannot have an empty suffix.");
                    } else {
                        ProngROBuilder roInterfaceBuilder = new ProngROBuilder(processingEnv, (TypeElement) element, annotation, new ReadonlyPropertyBuilderFactory());
                        roInterfaceBuilder.write("    ");

                        ProngStructBuilder structBuilder = new ProngStructBuilder(processingEnv, (TypeElement) element, annotation, new PojoPropertyBuilderFactory());
                        structBuilder.write("    ");
                    }
                } else {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, element + " must be interface or abstract class.", element);
                }
            }
        }
        catch (Throwable e) {
            StringWriter writer = new StringWriter();
            debugError("Internal ", e, writer);
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, writer.toString());
        }
        return true;
    }

    public static void debugError(final String message, final Throwable th, Writer logWriter) {
        final String logMessage = message;

        try {
            logWriter.write(logMessage);
            //logWriter.newLine();

            // dump exception stack if specified
            if (null != th) {
                final StackTraceElement[] traces = th.getStackTrace();
                if (null != traces && traces.length > 0) {
                    logWriter.write(th.getClass() + ": " + th.getMessage());
                    //logWriter.newLine();

                    for (final StackTraceElement trace : traces) {
                        logWriter.write("    at " + trace.getClassName() + '.' + trace.getMethodName() + '(' + trace.getFileName() + ':' + trace.getLineNumber() + ')');
                        //logWriter.newLine();
                    }
                }

                Throwable cause = th.getCause();
                while (null != cause) {
                    final StackTraceElement[] causeTraces = cause.getStackTrace();
                    if (null != causeTraces && causeTraces.length > 0) {
                        logWriter.write("Caused By:");
                        //logWriter.newLine();
                        logWriter.write(cause.getClass() + ": " + cause.getMessage());
                        //logWriter.newLine();

                        for (final StackTraceElement causeTrace : causeTraces) {
                            logWriter.write("    at " + causeTrace.getClassName() + '.' + causeTrace.getMethodName() + '(' + causeTrace.getFileName() + ':' + causeTrace.getLineNumber() + ')');
                            //logWriter.newLine();
                        }
                    }

                    // fetch next cause
                    cause = cause.getCause();
                }
            }
        } catch (final IOException ex) {
            System.err.println(logMessage);

            if (null != th) {
                th.printStackTrace();
            }
        }
    }
}
