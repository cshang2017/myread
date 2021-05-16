package org.apache.flink.runtime.types;

import com.twitter.chill.IKryoRegistrar;
import com.twitter.chill.java.ArraysAsListSerializer;
import com.twitter.chill.java.BitSetSerializer;
import com.twitter.chill.java.InetSocketAddressSerializer;
import com.twitter.chill.java.IterableRegistrar;
import com.twitter.chill.java.LocaleSerializer;
import com.twitter.chill.java.RegexSerializer;
import com.twitter.chill.java.SimpleDateFormatSerializer;
import com.twitter.chill.java.SqlDateSerializer;
import com.twitter.chill.java.SqlTimeSerializer;
import com.twitter.chill.java.TimestampSerializer;
import com.twitter.chill.java.URISerializer;
import com.twitter.chill.java.UUIDSerializer;

/*
This code is copied as is from Twitter Chill 0.7.4 because we need to user a newer chill version
but want to ensure that the serializers that are registered by default stay the same.

The only changes to the code are those that are required to make it compile and pass checkstyle
checks in our code base.
 */

/**
 * Creates a registrar for all the serializers in the chill.java package.
 */
public class FlinkChillPackageRegistrar {

	public static IKryoRegistrar all() {
		return new IterableRegistrar(
				ArraysAsListSerializer.registrar(),
				BitSetSerializer.registrar(),
				PriorityQueueSerializer.registrar(),
				RegexSerializer.registrar(),
				SqlDateSerializer.registrar(),
				SqlTimeSerializer.registrar(),
				TimestampSerializer.registrar(),
				URISerializer.registrar(),
				InetSocketAddressSerializer.registrar(),
				UUIDSerializer.registrar(),
				LocaleSerializer.registrar(),
				SimpleDateFormatSerializer.registrar());
	}
}
