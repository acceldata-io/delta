/*
 * Copyright (2022) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.storage.internal;

import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


/**
 * Static utility methods for the S3SingleDriverLogStore.
 *
 * Used to trick the class loader so we can use methods of org.apache.hadoop:hadoop-aws without needing to load this as
 * a dependency for tests in core.
 *
 * NOTE: The optimized S3A listing APIs (using startAfter parameter) require Hadoop 3.3.1+.
 * For Hadoop 3.2.x, this class throws UnsupportedOperationException and the caller
 * (S3SingleDriverLogStore) will fall back to using standard fs.listStatus().
 */
public final class S3LogStoreUtil {
    private S3LogStoreUtil() {}

    /**
     * Uses the S3ListRequest.v2 interface with the startAfter parameter to only list files
     * which are lexicographically greater than resolvedPath.
     *
     * Wraps s3ListFrom in an array. Contained in this class to avoid contaminating other
     * classes with dependencies on recent Hadoop versions.
     *
     * NOTE: This optimized implementation requires Hadoop 3.3.1+ internal S3A APIs:
     * - S3AUtils.iteratorToStatuses
     * - S3AUtils.intOption (public access)
     * - S3AFileSystem.getListing()
     * - S3AFileSystem.getActiveAuditSpan()
     * - Listing.AcceptAllButSelfAndS3nDirs (public access)
     *
     * For Hadoop 3.2.x, these APIs are not available, so this method throws
     * UnsupportedOperationException. The caller should catch this and fall back
     * to using standard fs.listStatus().
     *
     * TODO: Remove this method when iterators are used everywhere.
     */
    public static FileStatus[] s3ListFromArray(
            FileSystem fs,
            Path resolvedPath,
            Path parentPath) throws IOException {
        // The optimized S3A listing using startAfter parameter requires Hadoop 3.3.1+
        // internal APIs that are not available in Hadoop 3.2.x.
        // Throw UnsupportedOperationException so the caller falls back to fs.listStatus().
        throw new UnsupportedOperationException(
                "Optimized S3A listing (s3ListFromArray) requires Hadoop 3.3.1+ internal APIs. " +
                "The S3SingleDriverLogStore will fall back to using standard fs.listStatus().");
    }

    /**
     * Get the key which is lexicographically right before key.
     * If the key is empty return null.
     * If the key ends in a null byte, remove the last byte.
     * Otherwise, subtract one from the last byte.
     */
    static String keyBefore(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        if(bytes.length == 0) return null;
        if(bytes[bytes.length - 1] > 0) {
            bytes[bytes.length - 1] -= 1;
            return new String(bytes, StandardCharsets.UTF_8);
        } else {
            return new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8);
        }
    }
}
