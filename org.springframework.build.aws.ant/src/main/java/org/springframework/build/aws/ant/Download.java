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
import java.util.ArrayList;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.types.FileSet;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Bucket;

/**
 * A member of the S3 ANT task for dealing with Amazon S3 download behavior. This operation will use the credentials
 * setup in its parent S3 task tag.
 * 
 * @author Ben Hale
 */
public class Download extends AbstractS3DownloadOperation {

    private String file;

    private final List<FileSet> fileSets = new ArrayList<FileSet>(1);

    private File toDir;

    private File toFile;

    /**
     * Optional parameter that corresponds to the source object key in S3
     * 
     * @param file The source object key in S3
     */
    public void setFile(String file) {
        this.file = file;
    }

    /**
     * Adds an optional fileSet to read files from.
     * 
     * @param fileSet The set of files to download
     */
    public void addFileSet(FileSet fileSet) {
        this.fileSets.add(fileSet);
    }

    /**
     * Optional parameter that corresponds to the target object directory
     * 
     * @param toDir The target object directory
     */
    public void setToDir(File toDir) {
        this.toDir = toDir;
    }

    /**
     * Required parameter that corresponds to the file to download
     * 
     * @param toFile The file to download
     */
    public void setToFile(File toFile) {
        this.toFile = toFile;
    }

    /**
     * Verify that required parameters have been set
     */
    public void init() {
        if (this.bucketName == null) {
            throw new BuildException("bucketName must be set");
        }
        if ((this.file != null) && (this.fileSets.size() > 0)) {
            throw new BuildException("Only one of file and <fileset> may be set");
        }
        if ((this.file == null) && (this.fileSets.size() == 0)) {
            throw new BuildException("At least one of file and <fileset> must be set");
        }
        if ((this.toFile != null) && (this.toDir != null)) {
            throw new BuildException("Only one of toFile and toDir may be set");
        }
        if ((this.toFile == null) && (this.toDir == null)) {
            throw new BuildException("At least one of toFile and toDir must be set");
        }
        if ((this.fileSets.size() > 0) && (this.toFile != null)) {
            throw new BuildException("toFile cannot be used when specifying a <fileset> to download");
        }
    }

    public void execute(S3Service service) throws ServiceException, IOException {
        if ((this.file != null) && (this.toFile != null)) {
            processFileToFile(service);
        } else if ((this.file != null) && (this.toDir != null)) {
            processFileToDir(service);
        } else if ((this.fileSets.size() > 0) && (this.toDir != null)) {
            processSetToDir(service);
        }
    }

    private void processFileToFile(S3Service service) throws ServiceException, IOException {
        getFile(service, getOperationBucket(), this.file, this.toFile);
    }

    private void processFileToDir(S3Service service) throws ServiceException, IOException {
        getFile(service, getOperationBucket(), this.file, new File(this.toDir, this.file.substring(this.file.lastIndexOf('/'))));
    }

    private void processSetToDir(S3Service service) throws ServiceException, IOException {
        S3Bucket bucket = getOperationBucket();
        for (FileSet fileSet : this.fileSets) {
            S3Scanner scanner = getS3Scanner(bucket, fileSet.mergePatterns(this.project), getS3SafeDirectory(fileSet.getDir()));
            List<String> keys = scanner.getQualifiyingKeys(service);
            for (String key : keys) {
                if (!key.endsWith("/")) {
                    getFile(service, bucket, key, new File(this.toDir, key.substring(getS3SafeDirectory(fileSet.getDir()).length())));
                }
            }
        }
    }
}
