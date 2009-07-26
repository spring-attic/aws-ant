/*
 * Copyright 2007-2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.FileSet;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

/**
 * A member of the S3 ANT task for dealing with Amazon S3 upload behavior. This
 * operation will use the credentials setup in its parent S3 task tag.
 * 
 * @author Ben Hale
 */
public class Upload extends AbstractS3Operation {

	private File file;

	private List<FileSet> fileSets = new ArrayList<FileSet>();

	private String toDir;

	private String toFile;

	private boolean publicRead = false;

	private Set<Metadata> metadatas = new HashSet<Metadata>();

	/**
	 * Optional parameter that corresponds to the file to upload
	 * @param file The file to upload
	 */
	public void setFile(File file) {
		this.file = file;
	}

	/**
	 * Adds an optional fileSet to read files from.
	 * @param fileSet The set of files to upload
	 */
	public void addFileSet(FileSet fileSet) {
		fileSets.add(fileSet);
	}

	/**
	 * Adds an optional piece of
	 * @param property
	 */
	public void addMetadata(Metadata metadata) {
		metadatas.add(metadata);
	}

	/**
	 * Optional parameter that corresponds to the target object 'directory' in
	 * S3
	 * @param toDir The target object 'directory' in S3
	 */
	public void setToDir(String toDir) {
		this.toDir = toDir;
	}

	/**
	 * Optional parameter that corresponds to the target object key in S3
	 * @param toFile The target object key in S3
	 */
	public void setToFile(String toFile) {
		this.toFile = toFile;
	}

	/**
	 * Optional parameter that corresponds to public readability of the object
	 * in S3. Defaults to false.
	 * @param publicRead
	 */
	public void setPublicRead(boolean publicRead) {
		this.publicRead = publicRead;
	}

	/**
	 * Verify that required parameters have been set
	 */
	public void init() {
		if (bucketName == null) {
			throw new BuildException("bucketName must be set");
		}
		if (file != null && fileSets.size() > 0) {
			throw new BuildException("Only one of file and <fileset> may be set");
		}
		if (file == null && fileSets.size() == 0) {
			throw new BuildException("At least one of file and <fileset> must be set");
		}
		if (toFile != null && toDir != null) {
			throw new BuildException("Only one of toFile and toDir may be set");
		}
		if (toFile == null && toDir == null) {
			throw new BuildException("At least one of toFile and toDir must be set");
		}
		if (fileSets.size() > 0 && toFile != null) {
			throw new BuildException("toFile cannot be used when specifying a <fileset> to upload");
		}
	}

	public void execute(S3Service service) throws S3ServiceException, IOException {
		if (file != null && toFile != null) {
			processFileToFile(service);
		}
		else if (file != null && toDir != null) {
			processFileToDir(service);
		}
		else if (fileSets.size() > 0 && toDir != null) {
			processSetToDir(service);
		}
	}

	private void processFileToFile(S3Service service) throws S3ServiceException, IOException {
		S3Bucket bucket = new S3Bucket(bucketName);
		putFile(service, bucket, file, toFile);
	}

	private void processFileToDir(S3Service service) throws S3ServiceException, IOException {
		S3Bucket bucket = new S3Bucket(bucketName);
		putFile(service, bucket, file, toDir + "/" + file.getName());
	}

	private void processSetToDir(S3Service service) throws S3ServiceException, IOException {
		for (FileSet fileSet : fileSets) {
			DirectoryScanner ds = fileSet.getDirectoryScanner(project);
			String[] includedFiles = ds.getIncludedFiles();
			for (String file : includedFiles) {
				putFile(service, getOperationBucket(), new File(ds.getBasedir(), file), toDir + "/" + file);
			}
		}
	}

	private void putFile(S3Service service, S3Bucket bucket, File source, String key) throws S3ServiceException,
			IOException {
		buildDestinationPath(service, bucket, getDestinationPath(key));

		S3Object destination = new S3Object(bucket, key);
		if (publicRead) {
			destination.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
		}
		destination.setDataInputFile(source);
		destination.setContentLength(source.length());
		
		for(Metadata metadata : metadatas) {
			destination.addMetadata(metadata.getName(), metadata.getValue());
		}

		logStart(source, destination);
		long startTime = System.currentTimeMillis();
		service.putObject(bucket, destination);
		long endTime = System.currentTimeMillis();
		logEnd(source, startTime, endTime);
	}

	private String getDestinationPath(String destination) {
		return destination.substring(0, destination.lastIndexOf('/'));
	}

	private void buildDestinationPath(S3Service service, S3Bucket bucket, String destination) throws S3ServiceException {
		S3Object object = new S3Object(bucket, destination + "/");
		if (publicRead) {
			object.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
		}
		object.setContentLength(0);
		service.putObject(bucket, object);

		int index = destination.lastIndexOf('/');
		if (index != -1) {
			buildDestinationPath(service, bucket, destination.substring(0, index));
		}
	}

	private void logStart(File source, S3Object destination) throws IOException {
		project.log("Uploading " + source.getCanonicalPath() + " (" + TransferUtils.getFormattedSize(source.length())
				+ ") to s3://" + destination.getBucketName() + "/" + destination.getKey(), Project.MSG_INFO);
	}

	private void logEnd(File source, long startTime, long endTime) {
		long transferTime = endTime - startTime;
		project.log("Transfer Time: " + TransferUtils.getFormattedTime(transferTime) + " - Transfer Rate: "
				+ TransferUtils.getFormattedSpeed(source.length(), transferTime), Project.MSG_INFO);
	}
}
