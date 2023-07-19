package org.sagebionetworks.migration;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.async.JobTarget;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.MigrationTypeCountDiff;
import org.sagebionetworks.migration.utils.MigrationTypeMetaDiff;
import org.sagebionetworks.migration.utils.ToolMigrationUtils;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRequest;
import org.sagebionetworks.repo.model.migration.HasMigrationType;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.util.Clock;

import com.google.inject.Inject;

public class ReporterImpl implements Reporter {

	private static final String CHECKSUMS_DO_NOT_MATCH = "CHECKSUMS DO NOT MATCH FOR: ";
	private static final String CHECKSUMS_MATCH = "Checksums match for: ";
	private static final String ELASE_MS_TEMPLATE = "%02d:%02d:%02d.%03d";
	private static final String WAITING_FOR_JOB_TEMPLATE = "%s job: %s state: %s on: %s type: '%s' elapse: %s";
	static final long ONE_SECOND_MS = 1000L;
	static final String COUNTDOWN_FORMAT = "Migration will start in %1$s seconds...";
	static final String STARTING_MIGRATION = "Starting migration...";

	Configuration configuration;
	Logger logger;
	Clock clock;

	@Inject
	public ReporterImpl(Configuration configuration, LoggerFactory loggerFactory, Clock clock) {
		super();
		this.configuration = configuration;
		this.logger = loggerFactory.getLogger(ReporterImpl.class);
		this.clock = clock;
	}

	@Override
	public void reportMetaDifferences(ResultPair<List<MigrationTypeCount>> typeCounts) {
		List<MigrationTypeMetaDiff> diffs = ToolMigrationUtils.getMigrationTypeMetaDiffs(
				typeCounts.getSourceResult(),
				typeCounts.getDestinationResult());
		for (MigrationTypeMetaDiff diff : diffs) {
			if (!diff.getMinReport().sourceEqualsDest() ||
					!diff.getMaxReport().sourceEqualsDest()) {
				logger.info(String.format("\t%s:\tmins:%s\tmaxes:%s", diff.getMigrationType().name(), diff.getMinReport().toString(), diff.getMaxReport().toString()));
			}
		}
	}

	@Override
	public void runCountDownBeforeStart() {
		long countDownMS = configuration.getDelayBeforeMigrationStartMS();
		while (countDownMS > 0) {
			logger.info(String.format(COUNTDOWN_FORMAT, TimeUnit.MILLISECONDS.toSeconds(countDownMS)));
			try {
				clock.sleep(ONE_SECOND_MS);
			} catch (InterruptedException e) {
				throw new AsyncMigrationException(e);
			}
			countDownMS -= ONE_SECOND_MS;
		}
		logger.info(STARTING_MIGRATION);
	}

	@Override
	public void reportChecksums(MigrationType type,ResultPair<MigrationTypeChecksum> checksums) {
		MigrationTypeChecksum source = checksums.getSourceResult();
		MigrationTypeChecksum destination = checksums.getDestinationResult();
		if(source.getChecksum().equals(destination.getChecksum())) {
			logger.info(CHECKSUMS_MATCH+type.name());
		}else {
			logger.warn(CHECKSUMS_DO_NOT_MATCH+type.name());
		}
	}

	@Override
	public void reportProgress(JobTarget jobTarget,
			AsynchronousJobStatus jobStatus) {
		AsyncMigrationRequest request = (AsyncMigrationRequest) jobStatus.getRequestBody();
		AdminRequest adminRequest = request.getAdminRequest();
		String typeName = "";
		if(adminRequest instanceof HasMigrationType) {
			HasMigrationType hmt = (HasMigrationType) adminRequest;
			if(hmt.getMigrationType() != null) {
				typeName = hmt.getMigrationType().name();
			}
		}
		long jobStratedOn = jobStatus.getStartedOn().getTime();
		long elapse = clock.currentTimeMillis() - jobStratedOn;
		String elapseString = formatElapse(elapse);
		logger.info(String.format(WAITING_FOR_JOB_TEMPLATE, typeName, jobStatus.getJobId(), jobStatus.getJobState().name(), jobTarget.name(),
				adminRequest.getClass().getSimpleName(), elapseString));
	}
	
	/**
	 * For the given elapse milliseconds to: 'hh:mm:ss:MMM'
	 * @param elapse
	 * @return
	 */
	public static String formatElapse(long elapse) {
		long hours = TimeUnit.MILLISECONDS.toHours(elapse);
		long mins = TimeUnit.MILLISECONDS.toMinutes(elapse) % 60;
		long sec = TimeUnit.MILLISECONDS.toSeconds(elapse) % 60;
		long ms = TimeUnit.MILLISECONDS.toMillis(elapse) % 1000;
		return String.format(ELASE_MS_TEMPLATE, hours, mins, sec, ms);
	}

}
