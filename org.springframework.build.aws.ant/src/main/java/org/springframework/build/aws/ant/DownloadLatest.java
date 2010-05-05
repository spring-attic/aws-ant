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
import java.util.Collections;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.types.FileSet;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;

/**
 * A member of the S3 ANT task for dealing with Amazon S3 download behavior.
 * This operation will use the credentials setup in its parent S3 task tag and
 * download the latest matching file.
 * 
 * @author Ben Hale
 */
public class DownloadLatest extends AbstractS3DownloadOperation {

	private List<FileSet> fileSets = new ArrayList<FileSet>(1);

	private File toDir;

	/**
	 * Adds an optional fileSet to read files from.
	 * @param fileSet The set of files to download
	 */
	public void addFileSet(FileSet fileSet) {
		fileSets.add(fileSet);
	}

	/**
	 * Optional parameter that corresponds to the target object directory
	 * @param toDir The target object directory
	 */
	public void setToDir(File toDir) {
		this.toDir = toDir;
	}

	/**
	 * Verify that required parameters have been set
	 */
	public void init() {
		if (bucketName == null) {
			throw new BuildException("bucketName must be set");
		}
		if (fileSets.size() == 0) {
			throw new BuildException("At least one <fileset> must be set");
		}
		if (toDir == null) {
			throw new BuildException("toDir must be set");
		}
	}

	public void execute(S3Service service) throws S3ServiceException, IOException {
		processSetToDir(service);
	}

	private void processSetToDir(S3Service service) throws S3ServiceException, IOException {
		S3Bucket bucket = getOperationBucket();
		for (FileSet fileSet : fileSets) {
			S3Scanner scanner = getS3Scanner(bucket, fileSet.mergePatterns(project), getS3SafeDirectory(fileSet
					.getDir()));
			List<String> keys = scanner.getQualifiyingKeys(service);
			Collections.sort(keys);
			String key = keys.get(keys.size() - 1);
			if (!key.endsWith("/")) {
				getFile(service, bucket, key, new File(toDir, key.substring(getS3SafeDirectory(fileSet.getDir())
						.length())));
			}
		}
	}
}
