package org.sagebionetworks.migration.simulation;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.migration.MigrationClient;
import org.sagebionetworks.migration.MigrationModule;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.config.SynapseConnectionInfo;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

public class StackSimulator extends AbstractModule {

	private int maxNumberOfThreads = 2;
	private int maximumBackupBatchSize = 10;
	private long workerTimeoutMs = 1000L;
	private int maxRetries = 2;
	private BackupAliasType backupAliasType = BackupAliasType.MIGRATION_TYPE_NAME;
	private boolean includeFullTableChecksums = false;
	private long delayBeforeMigrationStartM = 10L;
	private boolean remainInReadOnlyAfterMigration = false;

	private final SimulatedStack sourceStack;
	private final SimulatedStack destinationStack;

	public StackSimulator(SimulatedStack sourceStack, SimulatedStack destinationStack) {
		super();
		this.sourceStack = sourceStack;
		this.destinationStack = destinationStack;
	}

	/**
	 * Create a {@link MigrationClient} that is configured to communicate with
	 * simulated source and destination stacks.
	 * 
	 * @return
	 */
	MigrationClient createClientWithSimulatedServices() {
		Injector injector = Guice.createInjector(Modules.override(new MigrationModule()).with(this));
		return injector.getInstance(MigrationClient.class);
	}

	@Override
	protected void configure() {
		bind(SynapseClientFactory.class).toInstance(new SynapseClientFactory() {

			@Override
			public SynapseAdminClient getSourceClient() {
				return sourceStack.createProxy();
			}

			@Override
			public SynapseAdminClient getDestinationClient() {
				return destinationStack.createProxy();
			}
		});
		bind(Configuration.class).toInstance(new Configuration() {

			@Override
			public SynapseConnectionInfo getSourceConnectionInfo() {
				return null;
			}

			@Override
			public SynapseConnectionInfo getDestinationConnectionInfo() {
				return null;
			}

			@Override
			public int getMaximumNumberThreads() {
				return maxNumberOfThreads;
			}

			@Override
			public int getMaximumBackupBatchSize() {
				return maximumBackupBatchSize;
			}

			@Override
			public long getWorkerTimeoutMs() {
				return workerTimeoutMs;
			}

			@Override
			public int getMaxRetries() {
				return maxRetries;
			}

			@Override
			public BackupAliasType getBackupAliasType() {
				return backupAliasType;
			}

			@Override
			public boolean includeFullTableChecksums() {
				return includeFullTableChecksums;
			}

			@Override
			public void logConfiguration() {

			}

			@Override
			public long getDelayBeforeMigrationStartMS() {
				return delayBeforeMigrationStartM;
			}

			@Override
			public boolean remainInReadOnlyAfterMigration() {
				return remainInReadOnlyAfterMigration;
			}

		});
	}
}
