/*
 * Copyright 2010, 2013 SpringSource
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
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.FileSet;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.multi.SimpleThreadedStorageService;

/**
 * A member of the S3 ANT task for dealing with Amazon S3 upload behavior. This operation will use the credentials setup
 * in its parent S3 task tag.
 * 
 * @author Ben Hale
 * @author Martin Lippert
 */
public class Upload extends AbstractS3Operation {

    private File file;

    private final List<FileSet> fileSets = new ArrayList<FileSet>();

    private String toDir;

    private String toFile;

    private boolean publicRead = false;

    private boolean multithreaded = false;

    private final Set<Metadata> metadatas = new HashSet<Metadata>();

    /**
     * Optional parameter that corresponds to the file to upload
     * 
     * @param file The file to upload
     */
    public void setFile(File file) {
        this.file = file;
    }

    /**
     * Adds an optional fileSet to read files from.
     * 
     * @param fileSet The set of files to upload
     */
    public void addFileSet(FileSet fileSet) {
        this.fileSets.add(fileSet);
    }

    /**
     * Adds an optional piece of
     * 
     * @param property
     */
    public void addMetadata(Metadata metadata) {
        this.metadatas.add(metadata);
    }

    /**
     * Optional parameter that corresponds to the target object 'directory' in S3
     * 
     * @param toDir The target object 'directory' in S3
     */
    public void setToDir(String toDir) {
        this.toDir = toDir;
    }

    /**
     * Optional parameter that corresponds to the target object key in S3
     * 
     * @param toFile The target object key in S3
     */
    public void setToFile(String toFile) {
        this.toFile = toFile;
    }

    /**
     * Optional parameter that corresponds to public readability of the object in S3. Defaults to false.
     * 
     * @param publicRead
     */
    public void setPublicRead(boolean publicRead) {
        this.publicRead = publicRead;
    }

    /**
     * Optional parameter that corresponds to multithreaded upload of all the S3 objects. Defaults to false.
     * 
     * @param multithreaded
     */
    public void setMultithreaded(boolean multithreaded) {
        this.multithreaded = multithreaded;
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
            throw new BuildException("toFile cannot be used when specifying a <fileset> to upload");
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
        putFile(service, getOperationBucket(), this.file, this.toFile);
    }

    private void processFileToDir(S3Service service) throws ServiceException, IOException {
        putFile(service, getOperationBucket(), this.file, this.toDir + "/" + this.file.getName());
    }

    private void processSetToDir(S3Service service) throws ServiceException, IOException {
        if (this.multithreaded) {
            processSetToDirMultiThreaded(service);
        } else {
            processSetToDirSingleThreaded(service);
        }
    }

    private void processSetToDirSingleThreaded(S3Service service) throws ServiceException, IOException {
        for (FileSet fileSet : this.fileSets) {
            DirectoryScanner ds = fileSet.getDirectoryScanner(this.project);
            String[] includedFiles = ds.getIncludedFiles();
            for (String file : includedFiles) {
                putFile(service, getOperationBucket(), new File(ds.getBasedir(), file), this.toDir + "/" + file);
            }
        }
    }

    private void processSetToDirMultiThreaded(S3Service service) throws ServiceException, IOException {
        List<S3Object> s3objects = new ArrayList<S3Object>();
        long totalLength = 0;

        this.project.log("Collecting objects for multithreaded upload to s3://" + getOperationBucket().getName(), Project.MSG_INFO);

        for (FileSet fileSet : this.fileSets) {
            DirectoryScanner ds = fileSet.getDirectoryScanner(this.project);
            String[] includedFiles = ds.getIncludedFiles();
            for (String file : includedFiles) {
                File sourceFile = new File(ds.getBasedir(), file);
                S3Object s3Object = createS3Object(service, getOperationBucket(), sourceFile, this.toDir + "/" + file);
                s3objects.add(s3Object);
                totalLength += sourceFile.length();

                logStart(sourceFile, s3Object);
            }
        }
        putFiles(service, getOperationBucket(), s3objects.toArray(new S3Object[s3objects.size()]), totalLength);
    }

    private void putFile(S3Service service, S3Bucket bucket, File source, String key) throws ServiceException, IOException {
        S3Object destination = createS3Object(service, bucket, source, key);

        logStart(source, destination);
        long startTime = System.currentTimeMillis();
        service.putObject(bucket, destination);
        long endTime = System.currentTimeMillis();
        logEnd(source.length(), startTime, endTime);
    }

    private void putFiles(S3Service service, S3Bucket bucket, S3Object[] s3objects, long totalLength) throws ServiceException {
        SimpleThreadedStorageService multiService = new SimpleThreadedStorageService(service);

        this.project.log("Starting multithreaded upload of " + s3objects.length + " objects to s3://" + bucket.getName(), Project.MSG_INFO);
        long startTime = System.currentTimeMillis();
        multiService.putObjects(bucket.getName(), s3objects);
        long endTime = System.currentTimeMillis();
        logEnd(totalLength, startTime, endTime);
    }

    private S3Object createS3Object(S3Service service, S3Bucket bucket, File source, String key) throws ServiceException, IOException {
        buildDestinationPath(service, bucket, getDestinationPath(key));

        S3Object destination = new S3Object(bucket, key);
        if (this.publicRead) {
            destination.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
        }
        destination.setDataInputFile(source);
        destination.setContentLength(source.length());

        for (Metadata metadata : this.metadatas) {
            destination.addMetadata(metadata.getName(), metadata.getValue());
        }
        return destination;
    }

    private String getDestinationPath(String destination) {
        return destination.substring(0, destination.lastIndexOf('/'));
    }

    private void buildDestinationPath(S3Service service, S3Bucket bucket, String destination) throws ServiceException, IOException {
        S3Object object;
        try {
            object = new S3Object(destination + "/", new byte[0]);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        if (this.publicRead) {
            object.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
        }
        service.putObject(bucket, object);

        int index = destination.lastIndexOf('/');
        if (index != -1) {
            buildDestinationPath(service, bucket, destination.substring(0, index));
        }
    }

    private void logStart(File source, S3Object destination) throws IOException {
        this.project.log("Uploading " + source.getCanonicalPath() + " (" + TransferUtils.getFormattedSize(source.length()) + ") to s3://"
            + destination.getBucketName() + "/" + destination.getKey(), Project.MSG_INFO);
    }

    private void logEnd(long sourceLength, long startTime, long endTime) {
        long transferTime = endTime - startTime;
        this.project.log(
            "Transfer Time: " + TransferUtils.getFormattedTime(transferTime) + " - Transfer Rate: "
                + TransferUtils.getFormattedSpeed(sourceLength, transferTime), Project.MSG_INFO);
    }
}
