package org.sagebionetworks.migration.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.LoggerFactory;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.inject.Inject;

/**
 * Provides configuration information 
 */
public class MigrationConfigurationImpl implements Configuration {

	static final String KEY_REMAIN_READ_ONLY_MODE = "org.sagebionetworks.remain.read.only.mode";
	static final String KEY_SERVICE_KEY = "org.sagebionetworks.service.key";
	static final String KEY_SOURCE_SERVICE_SECRET = "org.sagebionetworks.service.secret.source";
	static final String KEY_DESTINATION_SERVICE_SECRET = "org.sagebionetworks.service.secret.destination";
	static final String KEY_MAX_THREADS = "org.sagebionetworks.max.threads";
	static final String KEY_MAX_BACKUP_BATCHSIZE = "org.sagebionetworks.max.backup.batchsize";
	static final String KEY_THREAD_TIMOUT_MS = "org.sagebionetworks.worker.thread.timout.ms";
	static final String KEY_MAX_RETRIES = "org.sagebionetworks.max.retries";
	static final String KEY_BACKUP_ALIAS_TYPE = "org.sagebionetworks.backup.alias.type";
	static final String KEY_DELAY_BEFORE_START_MS = "org.sagebionetworks.delay.before.start.ms";
	static final String KEY_INCLUDE_FULL_TABLE_CHECKSUM = "org.sagebionetworks.include.full.table.checksum";
	static final String KEY_STACK = "org.sagebionetworks.stack";
	static final String REPO_ENDPOINT_FORMAT = "repo-%s.%s.sagebase.org/%s/v1";

	enum StackType {
		PROD ("prod"),
		STAGING ("staging");

		private final String label;

		private StackType(String s) {
			label = s;
		}

		public String toString() {
			return this.label;
		}

	}

	enum EndpointType {
		REPO ("repo"),
		AUTH ("auth");

		private final String label;

		private EndpointType(String s) {
			label = s;
		}

		public String toString() {
			return this.label;
		}
	}

	Logger logger;
	SystemPropertiesProvider propProvider;
	FileProvider fileProvider;
	AWSSecretsManager secretManager;
	
	Properties systemProperties;

	private String buildRepoEndpoint(String stack, StackType stackType, EndpointType endpointType) {
		return String.format(REPO_ENDPOINT_FORMAT, stackType.toString(), stack, endpointType.toString());
	}

	@Inject
	public MigrationConfigurationImpl(LoggerFactory loggerFactory, SystemPropertiesProvider propProvider, FileProvider fileProvider, AWSSecretsManager secretManager) throws IOException {
		this.logger = loggerFactory.getLogger(MigrationConfigurationImpl.class);
		this.propProvider = propProvider;
		this.fileProvider = fileProvider;
		this.secretManager = secretManager;
		// load the the System properties.
		systemProperties = propProvider.getSystemProperties();
	}

	@Override
	public SynapseConnectionInfo getSourceConnectionInfo(){
		return new SynapseConnectionInfo(
					buildRepoEndpoint(getProperty(KEY_STACK), StackType.PROD,  EndpointType.AUTH),
					buildRepoEndpoint(getProperty(KEY_STACK), StackType.PROD,  EndpointType.REPO),
					getProperty(KEY_SERVICE_KEY),
					getSecret(KEY_SOURCE_SERVICE_SECRET)
				);
	}
	
	@Override
	public SynapseConnectionInfo getDestinationConnectionInfo(){
		return new SynapseConnectionInfo(
					buildRepoEndpoint(getProperty(KEY_STACK), StackType.STAGING,  EndpointType.AUTH),
					buildRepoEndpoint(getProperty(KEY_STACK), StackType.STAGING,  EndpointType.REPO),
					getProperty(KEY_SERVICE_KEY),
					getSecret(KEY_DESTINATION_SERVICE_SECRET)
				);
	}
	
	@Override
	public int getMaximumNumberThreads() {
		return Integer.parseInt(getProperty(KEY_MAX_THREADS));
	}
	
	@Override
	public int getMaximumBackupBatchSize(){
		return Integer.parseInt(getProperty(KEY_MAX_BACKUP_BATCHSIZE));
	}

	@Override
	public long getWorkerTimeoutMs(){
		return Long.parseLong(getProperty(KEY_THREAD_TIMOUT_MS));
	}

	@Override
	public int getMaxRetries() {
		return Integer.parseInt(getProperty(KEY_MAX_RETRIES));
	}

	@Override
	public BackupAliasType getBackupAliasType() {
		return BackupAliasType.valueOf(getProperty(KEY_BACKUP_ALIAS_TYPE));
	}
	
	@Override
	public boolean includeFullTableChecksums() {
		return Boolean.parseBoolean(getProperty(KEY_INCLUDE_FULL_TABLE_CHECKSUM));
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	String getProperty(String key) {
		String value = this.systemProperties.getProperty(key);
		if(value == null) {
			throw new IllegalArgumentException("Missing system property: "+key);
		}
		return value;
	}
	
	/**
	 * Load the the properties from the given file path.
	 * @param path
	 * @return
	 * @throws IOException
	 */
	Properties loadPropertiesFromPath(String path) throws IOException {
		File file = fileProvider.getFile(path);
		if(!file.exists()) {
			throw new IllegalArgumentException("The property file does not exist:"+path);
		}
		InputStream fis = null;
		try{
			fis = fileProvider.createInputStream(file);
			Properties props = this.propProvider.createNewProperties();
			props.load(fis);
			return props;
		}finally{
			fis.close();
		}
	}
	
	@Override
	public long getDelayBeforeMigrationStartMS() {
		return Long.parseLong(getProperty(KEY_DELAY_BEFORE_START_MS));
	}
	
	@Override
	public void logConfiguration() {
		logger.info("Source: "+getSourceConnectionInfo().toString());
		logger.info("Destination: "+getDestinationConnectionInfo().toString());
		logger.info("Max number of retries: "+getMaxRetries());
		logger.info("Batch size: "+getMaximumBackupBatchSize());
		logger.info("BackupAliasType: "+getBackupAliasType());
		logger.info("Include full table checksums: "+includeFullTableChecksums());
		logger.info("Asynchronous job timeout MS: "+getWorkerTimeoutMs());
		logger.info("Delay before migration starts MS: "+getDelayBeforeMigrationStartMS());
	}
	
	/**
	 * Get a secret given the secret key.
	 * 
	 * @param secretId
	 * @return
	 */
	String getSecret(String secretId) {
		GetSecretValueResult result = secretManager.getSecretValue(new GetSecretValueRequest().withSecretId(secretId));
		return result.getSecretString();
	}

	@Override
	public boolean remainInReadOnlyAfterMigration() {
		try {
			return Boolean.parseBoolean(getProperty(KEY_REMAIN_READ_ONLY_MODE));
		}catch(IllegalArgumentException e) {
			// if the property is not set then return false.
			return false;
		}
	}
}
