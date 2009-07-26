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

import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Base class with utility methods for all transfer type services.
 * 
 * @author Ben Hale
 */
abstract class TransferUtils {

	private static final float KILOBYTE = 1024;

	private static final float MEGABYTE = 1048576;

	private static final float SECOND = 1000;

	private static final NumberFormat formatter = new DecimalFormat("###,###.0");
	
	private TransferUtils() {
	}

	public static String getFormattedSize(long size) {
		StringBuilder sb = new StringBuilder();
		float megabytes = size / MEGABYTE;
		if (megabytes > 1) {
			sb.append(formatter.format(megabytes));
			sb.append(" MB");
		}
		else {
			float kilobytes = size / KILOBYTE;
			sb.append(formatter.format(kilobytes));
			sb.append(" KB");
		}
		return sb.toString();
	}

	public static String getFormattedTime(long time) {
		StringBuilder sb = new StringBuilder();
		float seconds = time / SECOND;
		sb.append(formatter.format(seconds));
		sb.append(" s");
		return sb.toString();
	}

	public static String getFormattedSpeed(long size, long time) {
		StringBuilder sb = new StringBuilder();
		float seconds = time / SECOND;
		float megabytes = size / MEGABYTE;
		float megabytesPerSecond = megabytes / seconds;
		if (megabytesPerSecond > 1) {
			sb.append(formatter.format(megabytesPerSecond));
			sb.append(" MB/s");
		}
		else {
			float kilobytes = size / KILOBYTE;
			float kilobytesPerSecond = kilobytes / seconds;
			sb.append(formatter.format(kilobytesPerSecond));
			sb.append(" KB/s");
		}
		return sb.toString();
	}

}
