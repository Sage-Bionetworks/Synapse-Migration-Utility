package org.sagebionetworks.migration.async.checksum;

import java.util.Iterator;

import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.migration.async.BackupJobExecutor;
import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

import com.google.inject.Inject;

public class RangeCheksumBuilderImpl implements RangeCheksumBuilder {
	
	AsynchronousJobExecutor asynchronousJobExecutor;
	BackupJobExecutor backupJobExecutor;
	long batchSize;
	
	@Inject
	public RangeCheksumBuilderImpl(AsynchronousJobExecutor asynchronousJobExecutor,
			BackupJobExecutor backupJobExecutor, Configuration config) {
		super();
		this.asynchronousJobExecutor = asynchronousJobExecutor;
		this.backupJobExecutor = backupJobExecutor;
		this.batchSize = config.getMaximumBackupBatchSize();
	}


	@Override
	public Iterator<DestinationJob> providerRangeCheck(TypeToMigrateMetadata metadata,
			String salt) {
		return new ChecksumRangeExecutor(asynchronousJobExecutor, backupJobExecutor, batchSize, metadata, salt);
	}

}
