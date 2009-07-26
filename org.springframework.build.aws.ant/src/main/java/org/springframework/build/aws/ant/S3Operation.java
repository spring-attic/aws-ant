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

import java.io.IOException;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;

/**
 * An generic execution interface for all of the S3 operations.
 */
public interface S3Operation {

	/**
	 * Execute an S3 operation
	 * @param service The S3 Service to execute against
	 * @throws S3ServiceException
	 * @throws IOException
	 */
	void execute(S3Service service) throws S3ServiceException, IOException;
}
