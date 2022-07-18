package org.sagebionetworks.migration.async.checksum;

import java.util.Iterator;

import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

public interface RangeCheksumBuilder {

	/**
	 * For the given migration type and ID range, compare the checksums of both the
	 * sources and destination. If the checksums do not match, n number of backup
	 * jobs will be executed to be restored on the destination. This builder
	 * provides an iterator over all of the resulting destination jobs.
	 * 
	 * @param metadata
	 * @param salt
	 * @return
	 */
	Iterator<DestinationJob> providerRangeCheck(TypeToMigrateMetadata metadata, String salt);
}
