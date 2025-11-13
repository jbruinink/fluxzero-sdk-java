#include <jni.h>
#include "io_fluxzero_compression_LZ4Codec.h"
#include "lz4.h"

JNIEXPORT jint JNICALL Java_io_fluxzero_compression_LZ4Codec_LZ4_1compress_1default
  (JNIEnv* env, jclass cls,
   jbyteArray srcA, jint srcOff, jint srcLen,
   jbyteArray dstA, jint dstOff, jint maxDstSize)
{
    jbyte* src = (*env)->GetPrimitiveArrayCritical(env, srcA, 0);
    jbyte* dst = (*env)->GetPrimitiveArrayCritical(env, dstA, 0);

    int written = LZ4_compress_default(
        (char*)src + srcOff,
        (char*)dst + dstOff,
        srcLen,
        maxDstSize);

    (*env)->ReleasePrimitiveArrayCritical(env, srcA, src, 0);
    (*env)->ReleasePrimitiveArrayCritical(env, dstA, dst, 0);

    return written;
}

JNIEXPORT jint JNICALL Java_io_fluxzero_compression_LZ4Codec_LZ4_1decompress_1safe
  (JNIEnv* env, jclass cls,
   jbyteArray srcA, jint srcOff, jint srcLen,
   jbyteArray dstA, jint dstOff, jint dstCap)
{
    jbyte* src = (*env)->GetPrimitiveArrayCritical(env, srcA, 0);
    jbyte* dst = (*env)->GetPrimitiveArrayCritical(env, dstA, 0);

    int read = LZ4_decompress_safe(
        (char*)src + srcOff,
        (char*)dst + dstOff,
        srcLen,
        dstCap
    );

    (*env)->ReleasePrimitiveArrayCritical(env, srcA, src, 0);
    (*env)->ReleasePrimitiveArrayCritical(env, dstA, dst, 0);

    return read;
}

JNIEXPORT jint JNICALL Java_io_fluxzero_compression_LZ4Codec_LZ4_1compressBound
  (JNIEnv* env, jclass cls, jint len)
{
    return LZ4_compressBound(len);
}
