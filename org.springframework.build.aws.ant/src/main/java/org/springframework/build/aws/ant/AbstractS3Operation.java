/*
 * Copyright 2010 SpringSource
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.build.aws.ant;

import java.io.File;
import java.io.IOException;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.PatternSet;
import org.jets3t.service.model.S3Bucket;

public abstract class AbstractS3Operation implements S3Operation {

    protected String bucketName;

    protected Project project;

    /**
     * Required parameter that corresponds to the S3 bucket to delete from
     * 
     * @param bucketName The name of the bucket
     */
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    /**
     * Infrastructure element
     * 
     * @param project The project this task is running in
     */
    public void setProject(Project project) {
        this.project = project;
    }

    /**
     * Returns a file scanner for an S3 repository.
     * 
     * @param bucket The bucket to scan
     * @param patterns The patterns to scan for
     * @param baseDirectory The 'directory' to start the scan at
     * @return An initialized scanner
     */
    protected S3Scanner getS3Scanner(S3Bucket bucket, PatternSet patterns, String baseDirectory) {
        return new S3Scanner(bucket, baseDirectory, patterns.getIncludePatterns(this.project), patterns.getExcludePatterns(this.project));
    }

    /**
     * Get the bucket for this operation
     * 
     * @return An S3 bucket
     */
    protected S3Bucket getOperationBucket() {
        return new S3Bucket(this.bucketName);
    }

    /**
     * Gets a 'directory' that is safe to use with S3. Due to some stupidity in ANT, a fileset will only pass back a dir
     * argument that is relative to the project root. This strips that project root part of that directory giving you
     * the intended path from the root of the bucket.
     * 
     * @param file The ANT supplied directory to be normalized
     * @return A path that has been stripped of it's project root and made safe for S3
     * @throws IOException
     */
    protected String getS3SafeDirectory(File file) throws IOException {
        String longPath = file.getCanonicalPath();

        String normalizedPath;
        if (!longPath.endsWith("/")) {
            normalizedPath = longPath + '/';
        } else {
            normalizedPath = longPath;
        }

        String unneededPath = this.project.getBaseDir().getCanonicalPath();
        return normalizedPath.substring(unneededPath.length() + 1).replace('\\', '/');
    }

}
