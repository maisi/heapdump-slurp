package com.maisi.heapdump;

import com.ibm.dtfj.image.CorruptDataException;
import com.ibm.dtfj.image.Image;
import com.ibm.dtfj.image.ImageAddressSpace;
import com.ibm.dtfj.image.ImageFactory;
import com.ibm.dtfj.image.ImageProcess;
import com.ibm.dtfj.java.JavaClass;
import com.ibm.dtfj.java.JavaHeap;
import com.ibm.dtfj.java.JavaObject;
import com.ibm.dtfj.java.JavaRuntime;
import com.ibm.dtfj.java.JavaThread;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class JavaHeapAnalyzer {
    private JavaHeapAnalyzer() {}

    private static final class Arguments {
        final String inputPath;
        final String formatLabel;
        final boolean listStrings;

        private Arguments(String inputPath, String formatLabel, boolean listStrings) {
            this.inputPath = inputPath;
            this.formatLabel = formatLabel;
            this.listStrings = listStrings;
        }

        static Arguments parse(String[] args) {
            String input = null;
            String format = "unknown";
            boolean listStrings = false;
            for (int i = 0; i < args.length; i++) {
                String value = args[i];
                switch (value) {
                    case "--input":
                    case "-i":
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException("Missing value for " + value);
                        }
                        input = args[++i];
                        break;
                    case "--format":
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException("Missing value for --format");
                        }
                        format = args[++i];
                        break;
                    case "--list-strings":
                        listStrings = true;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + value);
                }
            }
            if (input == null) {
                throw new IllegalArgumentException("Input path is required (use --input)");
            }
            return new Arguments(input, format, listStrings);
        }
    }

    private static final class ClassStats {
        long instanceCount = 0L;
        long totalSize = 0L;
        long maxSize = 0L;
    }

    private static final class HelperResult {
        final Map<String, ClassStats> classStats;
        final long totalObjects;
        final long totalHeapBytes;
        final long stringCount;
        final int threadCount;

        HelperResult(Map<String, ClassStats> classStats, long totalObjects, long totalHeapBytes, long stringCount, int threadCount) {
            this.classStats = classStats;
            this.totalObjects = totalObjects;
            this.totalHeapBytes = totalHeapBytes;
            this.stringCount = stringCount;
            this.threadCount = threadCount;
        }
    }

    public static void main(String[] args) {
        try {
            Arguments arguments = Arguments.parse(args);
            HelperResult helperResult = analyze(arguments);
            emitJson(helperResult, arguments.formatLabel);
        } catch (Exception ex) {
            PrintStream err = System.err;
            ex.printStackTrace(err);
            System.exit(1);
        }
    }

    private static HelperResult analyze(Arguments arguments) throws Exception {
        Map<String, ClassStats> stats = new HashMap<>();
        long totalObjects = 0L;
        long totalHeapBytes = 0L;
        long stringCount = 0L;
        int threadCount = 0;

        ImageFactory factory = new com.ibm.dtfj.image.j9.ImageFactory();
        File inputFile = new File(arguments.inputPath);
        Image image;
        if ("openj9-core".equals(arguments.formatLabel)) {
            Image temp = null;
            try {
                Image[] images = factory.getImagesFromArchive(inputFile, true);
                if (images != null && images.length > 0) {
                    temp = images[0];
                }
            } catch (Exception ignored) {
                temp = null;
            }
            image = temp != null ? temp : factory.getImage(inputFile);
        } else {
            image = factory.getImage(inputFile);
        }
        try {
            Iterator<?> addressSpaces = image.getAddressSpaces();
            while (addressSpaces.hasNext()) {
                Object spaceCandidate = addressSpaces.next();
                if (!(spaceCandidate instanceof ImageAddressSpace)) {
                    continue;
                }
                ImageAddressSpace addressSpace = (ImageAddressSpace) spaceCandidate;
                Iterator<?> processes = addressSpace.getProcesses();
                while (processes.hasNext()) {
                    Object processCandidate = processes.next();
                    if (!(processCandidate instanceof ImageProcess)) {
                        continue;
                    }
                    ImageProcess process = (ImageProcess) processCandidate;
                    Iterator<?> runtimes = process.getRuntimes();
                    while (runtimes.hasNext()) {
                        Object runtimeCandidate = runtimes.next();
                        if (!(runtimeCandidate instanceof JavaRuntime)) {
                            continue;
                        }
                        JavaRuntime runtime = (JavaRuntime) runtimeCandidate;
                        threadCount += countThreads(runtime);
                        Iterator<?> heaps = runtime.getHeaps();
                        while (heaps != null && heaps.hasNext()) {
                            Object heapCandidate = heaps.next();
                            if (!(heapCandidate instanceof JavaHeap)) {
                                continue;
                            }
                            JavaHeap heap = (JavaHeap) heapCandidate;
                            Iterator<?> objects = heap.getObjects();
                            while (objects != null && objects.hasNext()) {
                                Object objectCandidate = objects.next();
                                if (!(objectCandidate instanceof JavaObject)) {
                                    continue;
                                }
                                JavaObject javaObject = (JavaObject) objectCandidate;
                                totalObjects++;
                                long size = safeObjectSize(javaObject);
                                if (size > 0) {
                                    totalHeapBytes += size;
                                }
                                JavaClass javaClass = safeClass(javaObject);
                                String className = renderClassName(javaClass);
                                if (className == null) {
                                    className = "<unknown>";
                                }
                                ClassStats classStats = stats.computeIfAbsent(className, key -> new ClassStats());
                                classStats.instanceCount++;
                                if (size > 0) {
                                    classStats.totalSize += size;
                                    if (size > classStats.maxSize) {
                                        classStats.maxSize = size;
                                    }
                                }
                                if ("java.lang.String".equals(className)) {
                                    stringCount++;
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            image.close();
        }

        return new HelperResult(stats, totalObjects, totalHeapBytes, stringCount, threadCount);
    }

    private static int countThreads(JavaRuntime runtime) {
        int count = 0;
        Iterator<?> threads = null;
        try {
            threads = runtime.getThreads();
        } catch (Exception ignored) {
            return 0;
        }
        while (threads != null && threads.hasNext()) {
            Object candidate = threads.next();
            if (candidate instanceof JavaThread) {
                count++;
            }
        }
        return count;
    }

    private static JavaClass safeClass(JavaObject object) {
        try {
            return object.getJavaClass();
        } catch (CorruptDataException ignored) {
            return null;
        }
    }

    private static long safeObjectSize(JavaObject object) {
        try {
            long size = object.getSize();
            return size >= 0 ? size : 0L;
        } catch (CorruptDataException unused) {
            return 0L;
        }
    }

    private static String renderClassName(JavaClass javaClass) {
        if (javaClass == null) {
            return null;
        }
        try {
            String raw = javaClass.getName();
            if (raw == null) {
                return null;
            }
            return prettifyTypeName(raw);
        } catch (CorruptDataException ignored) {
            return "<corrupt>";
        }
    }

    private static String prettifyTypeName(String rawName) {
        if (rawName.isEmpty()) {
            return rawName;
        }
        if (rawName.charAt(0) != '[') {
            return rawName.replace('/', '.');
        }
        int depth = 0;
        while (depth < rawName.length() && rawName.charAt(depth) == '[') {
            depth++;
        }
        if (depth >= rawName.length()) {
            return rawName;
        }
        char descriptor = rawName.charAt(depth);
        String base;
        switch (descriptor) {
            case 'B':
                base = "byte";
                break;
            case 'C':
                base = "char";
                break;
            case 'D':
                base = "double";
                break;
            case 'F':
                base = "float";
                break;
            case 'I':
                base = "int";
                break;
            case 'J':
                base = "long";
                break;
            case 'S':
                base = "short";
                break;
            case 'Z':
                base = "boolean";
                break;
            case 'L':
                int end = rawName.indexOf(';', depth);
                if (end == -1) {
                    base = rawName.substring(depth + 1).replace('/', '.');
                } else {
                    base = rawName.substring(depth + 1, end).replace('/', '.');
                }
                break;
            default:
                base = rawName;
                break;
        }
        StringBuilder builder = new StringBuilder(base.length() + depth * 2);
        builder.append(base);
        for (int i = 0; i < depth; i++) {
            builder.append("[]");
        }
        return builder.toString();
    }

    private static void emitJson(HelperResult helperResult, String formatLabel) {
        List<Map.Entry<String, ClassStats>> entries = new ArrayList<>(helperResult.classStats.entrySet());
        entries.sort(Comparator.comparingLong((Map.Entry<String, ClassStats> entry) -> entry.getValue().totalSize).reversed());

        StringBuilder builder = new StringBuilder(Math.max(1024, entries.size() * 96));
        builder.append('{');
        builder.append("\"memory_usage\":[");
        boolean first = true;
        for (Map.Entry<String, ClassStats> entry : entries) {
            if (!first) {
                builder.append(',');
            }
            first = false;
            ClassStats stats = entry.getValue();
            builder.append('{');
            builder.append("\"class_name\":\"").append(escape(entry.getKey())).append('\"');
            builder.append(',');
            builder.append("\"instance_count\":").append(stats.instanceCount);
            builder.append(',');
            builder.append("\"largest_allocation_bytes\":").append(stats.maxSize);
            builder.append(',');
            builder.append("\"allocation_size_bytes\":").append(stats.totalSize);
            builder.append('}');
        }
        builder.append(']');
        builder.append(',');
        builder.append("\"total_objects\":").append(helperResult.totalObjects);
        builder.append(',');
        builder.append("\"class_count\":").append(helperResult.classStats.size());
        builder.append(',');
        builder.append("\"thread_count\":").append(helperResult.threadCount);
        builder.append(',');
        builder.append("\"string_count\":").append(helperResult.stringCount);
        builder.append(',');
        builder.append("\"total_heap_bytes\":").append(helperResult.totalHeapBytes);
        builder.append(',');
        builder.append("\"format\":\"").append(escape(formatLabel)).append('\"');
        builder.append('}');
        System.out.println(builder.toString());
    }

    private static String escape(String value) {
        StringBuilder builder = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                    builder.append("\\\\");
                    break;
                case '"':
                    builder.append("\\\"");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        builder.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        builder.append(c);
                    }
                    break;
            }
        }
        return builder.toString();
    }
}
