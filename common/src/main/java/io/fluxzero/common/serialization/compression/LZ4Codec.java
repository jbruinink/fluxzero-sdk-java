package io.fluxzero.common.serialization.compression;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Locale;

/**
 * JNI-based LZ4 codec with a 4-byte big-endian length prefix.
 * <p>
 * Data format:
 * [0..3]   = original uncompressed length (int, big-endian)
 * [4..end] = raw LZ4-compressed bytes
 * <p>
 * The JNI library must expose:
 * - nCompressBound
 * - nCompress
 * - nDecompress
 */
public final class LZ4Codec {

    static {
        loadNativeLibrary();
    }

    private LZ4Codec() {
    }

    private static native int nCompressBound(int inputSize);

    private static native int nCompress(byte[] src, int srcLen, byte[] dst, int maxDstLen);

    private static native int nDecompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff, int maxDstLen);

    /**
     * Compresses the given data using LZ4 and prefixes it with a 4-byte
     * big-endian integer containing the original size.
     * @param input the data to compress
     * @return the compressed data, prefixed with the original size
     */
    public static byte[] compress(byte[] input) {
        if (input == null) {
            throw new NullPointerException("input");
        }

        int originalSize = input.length;
        int maxSize = nCompressBound(originalSize);
        if (maxSize <= 0) {
            throw new IllegalStateException("LZ4_compressBound returned " + maxSize);
        }

        byte[] compressed = new byte[maxSize];
        int written = nCompress(input, originalSize, compressed, maxSize);

        if (written <= 0) {
            throw new IllegalStateException("LZ4_compress_default failed, code=" + written);
        }

        byte[] out = new byte[written + 4];
        ByteBuffer.wrap(out).order(ByteOrder.BIG_ENDIAN).putInt(originalSize);
        System.arraycopy(compressed, 0, out, 4, written);
        return out;
    }

    /**
     * Decompresses LZ4 data produced by {@link #compress(byte[])},
     * using the 4-byte original size prefix.
     */
    public static byte[] decompress(byte[] compressed) {
        if (compressed == null) {
            throw new NullPointerException("compressed");
        }
        if (compressed.length < 4) {
            throw new IllegalArgumentException("Compressed block too small to contain header");
        }

        int originalSize = ByteBuffer.wrap(compressed, 0, 4).order(ByteOrder.BIG_ENDIAN).getInt();

        if (originalSize < 0) {
            throw new IllegalArgumentException("Invalid original size: " + originalSize);
        }

        byte[] out = new byte[originalSize];
        int compressedSize = compressed.length - 4;
        int written = nDecompress(compressed, 4, compressedSize, out, 0, originalSize);

        if (written < 0) {
            throw new IllegalStateException("LZ4_decompress_safe failed, code=" + written);
        }

        if (written == originalSize) {
            return out;
        }

        byte[] trimmed = new byte[written];
        System.arraycopy(out, 0, trimmed, 0, written);
        return trimmed;
    }

    private static void loadNativeLibrary() {
        String os = detectOS();
        String arch = detectArch();

        String dir = switch (os) {
            case "linux" -> "linux-" + arch;
            case "macos" -> "macos-" + arch;
            case "windows" -> "windows-" + arch;
            default -> throw new IllegalStateException("Unsupported OS: " + os);
        };

        String libFileName = switch (os) {
            case "linux" -> "lz4jni.so";
            case "macos" -> "lz4jni.dylib";
            case "windows" -> "lz4jni.dll";
            default -> throw new IllegalStateException("Unsupported OS: " + os);
        };

        String resourcePath = "/native/" + dir + "/" + libFileName;

        try {
            Path extracted = extractResource(resourcePath);
            System.load(extracted.toAbsolutePath().toString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load native library from " + resourcePath, e);
        }
    }

    private static String detectOS() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        if (os.contains("linux")) return "linux";
        if (os.contains("mac") || os.contains("darwin")) return "macos";
        if (os.contains("win")) return "windows";
        throw new IllegalStateException("Unsupported OS: " + os);
    }

    private static String detectArch() {
        String arch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);
        if (arch.contains("x86_64") || arch.contains("amd64")) return "x86_64";
        if (arch.contains("aarch64") || arch.contains("arm64")) return "aarch64";
        throw new IllegalStateException("Unsupported architecture: " + arch);
    }

    private static Path extractResource(String resource) throws IOException {
        try (InputStream in = LZ4Codec.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new FileNotFoundException("Native library not found on classpath: " + resource);
            }

            Path tmp = Files.createTempFile("lz4jni-", null);
            tmp.toFile().deleteOnExit();
            Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
            return tmp;
        }
    }
}
