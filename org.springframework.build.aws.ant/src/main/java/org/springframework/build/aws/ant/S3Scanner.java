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

import java.util.ArrayList;
import java.util.List;

import org.apache.tools.ant.types.selectors.SelectorUtils;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

class S3Scanner {

    private final S3Bucket bucket;

    private final String baseDirectory;

    private final List<String> includePatterns;

    private final List<String> excludePatterns;

    /**
     * Creates a new instance of a scanner for an S3 repository.
     * 
     * @param bucket The bucket to scan in
     * @param baseDirectory The base 'directory' to scan in
     * @param inlcudePatterns The include patterns to scan for
     * @param excludePatterns The exclude patterns to scan for
     */
    public S3Scanner(S3Bucket bucket, String baseDirectory, String[] includePatterns, String[] excludePatterns) {
        this.bucket = bucket;
        this.baseDirectory = baseDirectory;

        if ((includePatterns == null) || (includePatterns.length == 0)) {
            this.includePatterns = normalizePatterns(new String[] { "**" });
        } else {
            this.includePatterns = normalizePatterns(includePatterns);
        }

        if (excludePatterns == null) {
            this.excludePatterns = normalizePatterns(new String[0]);
        } else {
            this.excludePatterns = normalizePatterns(excludePatterns);
        }
    }

    /**
     * Returns a list of keys that qualify the include and exclude patterns specified.
     * 
     * @param service The S3 service to use for scanning
     * @return The list of qualifying keys
     * @throws S3ServiceException
     */
    public List<String> getQualifiyingKeys(S3Service service) throws S3ServiceException {
        List<String> qualifying = new ArrayList<String>();

        S3Object[] candidates = service.listObjects(this.bucket.getName(), this.baseDirectory, "");
        for (S3Object candidate : candidates) {
            String trimmedCandidate = candidate.getKey().substring(this.baseDirectory.length());
            if (matchesInclude(trimmedCandidate) && !matchesExclude(trimmedCandidate)) {
                qualifying.add(candidate.getKey());
            }
        }

        return qualifying;
    }

    private List<String> normalizePatterns(String[] patterns) {
        List<String> normalizedPatterns = new ArrayList<String>(patterns.length);
        for (String pattern : patterns) {
            normalizedPatterns.add(normalizePattern(pattern));
        }
        return normalizedPatterns;
    }

    private String normalizePattern(String pattern) {
        if (pattern.endsWith("/")) {
            return pattern + "**";
        }
        return pattern;
    }

    private boolean matchesInclude(String candidate) {
        return matches(this.includePatterns, candidate);
    }

    private boolean matchesExclude(String candidate) {
        return matches(this.excludePatterns, candidate);
    }

    private boolean matches(List<String> patterns, String candidate) {
        for (String pattern : patterns) {
            if (SelectorUtils.matchPath(pattern, candidate)) {
                return true;
            }
        }
        return false;
    }
}
