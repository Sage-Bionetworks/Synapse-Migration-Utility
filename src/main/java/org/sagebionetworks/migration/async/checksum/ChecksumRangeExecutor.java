package org.sagebionetworks.migration.async.checksum;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.migration.async.BackupJobExecutor;
import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.BatchChecksumRequest;
import org.sagebionetworks.repo.model.migration.BatchChecksumResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.RangeChecksum;

/**
 * This executor will first compare the checksums from both the source and
 * destination for the given ID range. If the checksums do not match, then n
 * number of backup jobs will be started to restore the entire range. If the
 * checksums match, no further work is required.
 * <p>
 * No work is done in the constructor of this object. Checksums will not be
 * executed until the first call to {@link #hasNext()}.
 *
 */
public class ChecksumRangeExecutor implements Iterator<DestinationJob> {

	private final AsynchronousJobExecutor asynchronousJobExecutor;
	private BackupJobExecutor backupJobExecutor;
	private Long batchSize;
	private TypeToMigrateMetadata metadata;
	private String salt;
	private Iterator<DestinationJob> lastBackupJobs;
	private Iterator<RangeChecksum> mismatchedRanges;

	/**
	 * No work is done in the constructor of this object. Checksums will not be
	 * executed until the first call to {@link #hasNext()}.
	 * 
	 * @param asynchronousJobExecutor
	 * @param backupJobExecutor
	 * @param type
	 * @param minimumId
	 * @param maximumId
	 * @param salt
	 */
	public ChecksumRangeExecutor(AsynchronousJobExecutor asynchronousJobExecutor, BackupJobExecutor backupJobExecutor,
			Long batchSize, TypeToMigrateMetadata metadata, String salt) {
		super();
		this.asynchronousJobExecutor = asynchronousJobExecutor;
		this.backupJobExecutor = backupJobExecutor;
		this.batchSize = batchSize;
		this.metadata = metadata;
		this.salt = salt;
		// start with an empty iterator.
		lastBackupJobs = new LinkedList<DestinationJob>().iterator();
	}

	@Override
	public boolean hasNext() {
		if (mismatchedRanges == null) {
			/*
			 * This is the first call so find all batches with mismatched checksums.
			 */
			this.mismatchedRanges = findAllMismatchedRanges();
		}
		if (lastBackupJobs.hasNext()) {
			return true;
		} else {
			// find the next set of restore jobs
			if (!mismatchedRanges.hasNext()) {
				// we are done.
				return false;
			}
			// Start n number of backup jobs for the mismatched ID range.
			RangeChecksum misMatchRange = mismatchedRanges.next();
			// Fix for PLFM-6551, the bin numbers need to drive the backup range.
			long binStart = misMatchRange.getBinNumber()*batchSize;
			long binEnd = binStart+batchSize-1;
			lastBackupJobs = backupJobExecutor.executeBackupJob(metadata.getType(), binStart, binEnd);
			return lastBackupJobs.hasNext();
		}
	}

	@Override
	public DestinationJob next() {
		return lastBackupJobs.next();
	}

	/**
	 * Find all checksum ranges that do not match on both the source and
	 * destination.
	 * 
	 * @return
	 */
	Iterator<RangeChecksum> findAllMismatchedRanges() {
		List<RangeChecksum> mismatchedRangesList = new LinkedList<>();
		if (metadata.getSrcMinId() != null) {
			BatchChecksumRequest request = new BatchChecksumRequest();
			request.setMigrationType(metadata.getType());
			request.setBatchSize(this.batchSize);
			request.setMinimumId(metadata.getSrcMinId());
			request.setMaximumId(metadata.getSrcMaxId());
			request.setSalt(this.salt);
			// get all checksums for this range from both the source and destination.
			ResultPair<BatchChecksumResponse> results = asynchronousJobExecutor.executeSourceAndDestinationJob(request,
					BatchChecksumResponse.class);
			mismatchedRangesList = findAllMismatchedRanges(results.getSourceResult().getCheksums(), results.getDestinationResult().getCheksums());
		}
		return mismatchedRangesList.iterator();
	}

	/**
	 * Find all of the mismatched ranges for the given source and destination checksums.
	 * 
	 * @param sourceResult
	 * @param destinationResult
	 * @return
	 */
	static List<RangeChecksum> findAllMismatchedRanges(List<RangeChecksum> sourceResult, List<RangeChecksum> destinationResult) {
		// Map destination bins to their checksums
		Map<Long, RangeChecksum> destinationBinToRange = new HashMap<>();
		if (destinationResult != null) {
			for (RangeChecksum range : destinationResult) {
				destinationBinToRange.put(range.getBinNumber(), range);
			}
		}
		List<RangeChecksum> mismatchRanges = new LinkedList<>();
		if (sourceResult != null) {
			for (RangeChecksum sourceChecksum : sourceResult) {
				// Find the matching destination checksum by bin
				RangeChecksum destinationChecksum = destinationBinToRange.remove(sourceChecksum.getBinNumber());
				if (destinationChecksum == null || !sourceChecksum.equals(destinationChecksum)) {
					// Checksums do not match
					mismatchRanges.add(sourceChecksum);
				}
			}
		}
		// anything left in destination map was not in source
		if(!destinationBinToRange.isEmpty()) {
			mismatchRanges.addAll(destinationBinToRange.values());
		}
		return mismatchRanges;
	}

}
