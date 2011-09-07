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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.tools.ant.Project;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

public abstract class AbstractS3DownloadOperation extends AbstractS3Operation {

    private static final int BUFFER_SIZE = 64 * 1024;

    protected void getFile(S3Service service, S3Bucket bucket, String key, File destination) throws ServiceException, IOException {
        InputStream in = null;
        OutputStream out = null;
        try {
            if (!destination.getParentFile().exists()) {
                destination.getParentFile().mkdirs();
            }

            S3Object source = service.getObject(bucket.getName(), key);
            in = source.getDataInputStream();
            out = new FileOutputStream(destination);

            logStart(source, destination);
            long startTime = System.currentTimeMillis();
            byte[] buffer = new byte[BUFFER_SIZE];
            int length;
            while ((length = in.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
            long endTime = System.currentTimeMillis();
            logEnd(source, startTime, endTime);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    // Nothing to do at this point
                }
            }
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    // Nothing to do at this point
                }
            }
        }
    }

    private void logStart(S3Object source, File destination) throws IOException {
        this.project.log(
            "Downloading s3://" + source.getBucketName() + "/" + source.getKey() + " (" + TransferUtils.getFormattedSize(source.getContentLength())
                + ") to " + destination.getCanonicalPath(), Project.MSG_INFO);
    }

    private void logEnd(S3Object source, long startTime, long endTime) {
        long transferTime = endTime - startTime;
        this.project.log(
            "Transfer Time: " + TransferUtils.getFormattedTime(transferTime) + " - Transfer Rate: "
                + TransferUtils.getFormattedSpeed(source.getContentLength(), transferTime), Project.MSG_INFO);
    }

}