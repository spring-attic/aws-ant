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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.FileSet;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Bucket;

/**
 * A member of the S3 ANT task for dealing with Amazon S3 delete behavior. This operation will use the credentials setup
 * in its parent S3 task tag.
 * 
 * @author Ben Hale
 */
public class Delete extends AbstractS3Operation {

    private String file;

    private final List<FileSet> fileSets = new ArrayList<FileSet>(1);

    /**
     * Optional parameter that corresponds to the source object key in S3
     * 
     * @param file The source object key in S3
     */
    public void setFile(String file) {
        this.file = file;
    }

    /**
     * Adds an optional fileSet to read delete from.
     * 
     * @param fileSet The set of files to delete
     */
    public void addFileSet(FileSet fileSet) {
        this.fileSets.add(fileSet);
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
    }

    public void execute(S3Service service) throws ServiceException, IOException {
        if (this.file != null) {
            processFile(service);
        } else if (this.fileSets.size() > 0) {
            processSet(service);
        }
    }

    private void processFile(S3Service service) throws ServiceException {
        deleteFile(service, getOperationBucket(), this.file);
    }

    private void processSet(S3Service service) throws IOException, ServiceException {
        S3Bucket bucket = getOperationBucket();
        for (FileSet fileSet : this.fileSets) {
            S3Scanner scanner = getS3Scanner(bucket, fileSet.mergePatterns(this.project), getS3SafeDirectory(fileSet.getDir()));
            List<String> keys = scanner.getQualifiyingKeys(service);
            for (String key : keys) {
                deleteFile(service, bucket, key);
            }
        }
    }

    private void deleteFile(S3Service service, S3Bucket bucket, String key) throws ServiceException {
        service.deleteObject(bucket, key);
        this.project.log("Deleted s3://" + bucket.getName() + "/" + key, Project.MSG_INFO);
    }
}
