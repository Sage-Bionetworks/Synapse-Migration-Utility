package org.sagebionetworks.migration.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.LoggerFactory;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;

@RunWith(MockitoJUnitRunner.class)
public class MigrationConfigurationImplTest {

	@Mock
	SystemPropertiesProvider mockPropertyProvider;
	@Mock
	FileProvider mockFileProvider;
	@Mock
	File mockFile;
	@Mock
	InputStream mockInputStream;
	@Mock
	Properties mockProperties;
	@Mock
	LoggerFactory mockLoggerFactory;
	@Mock
	Logger mockLogger;
	@Mock
	AWSSecretsManager mockSecretManager;
	
	MigrationConfigurationImpl config;
	
	String sampleKey;
	String sampleValue;
	String serviceKey;
	String sourceServiceSecret;
	String destinationServiceSecret;
	Properties props;
	
	@Before
	public void before() throws IOException {
		
		sampleKey = "sampleKey";
		sampleValue = "sampleValue";
		serviceKey = "migration";
		sourceServiceSecret = "sourceKeySecret";
		destinationServiceSecret = "destinationKeySecret";

		props = new Properties();
		props.put(sampleKey, sampleValue);
		props.put(MigrationConfigurationImpl.KEY_SERVICE_KEY, serviceKey);
		props.put(MigrationConfigurationImpl.KEY_MAX_BACKUP_BATCHSIZE, "2");
		props.put(MigrationConfigurationImpl.KEY_MAX_RETRIES, "3");
		props.put(MigrationConfigurationImpl.KEY_BACKUP_ALIAS_TYPE, BackupAliasType.TABLE_NAME.name());
		props.put(MigrationConfigurationImpl.KEY_INCLUDE_FULL_TABLE_CHECKSUM, "true");
		props.put(MigrationConfigurationImpl.KEY_DELAY_BEFORE_START_MS, "30000");
		props.put(MigrationConfigurationImpl.KEY_THREAD_TIMOUT_MS, "100000000");

		when(mockPropertyProvider.getSystemProperties()).thenReturn(props);
		when(mockPropertyProvider.createNewProperties()).thenReturn(mockProperties);

		when(mockFileProvider.getFile(anyString())).thenReturn(mockFile);
		when(mockFileProvider.createInputStream(any(File.class))).thenReturn(mockInputStream);
		when(mockFile.exists()).thenReturn(true);
		when(mockLoggerFactory.getLogger(any())).thenReturn(mockLogger);

		config = new MigrationConfigurationImpl(mockLoggerFactory, mockPropertyProvider, mockFileProvider, mockSecretManager);
	}

	private void setupMockSecretManager() {
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_SOURCE_SERVICE_SECRET)))
				.thenReturn(new GetSecretValueResult().withSecretString(sourceServiceSecret));
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_DESTINATION_SERVICE_SECRET)))
				.thenReturn(new GetSecretValueResult().withSecretString(destinationServiceSecret));
	}

	@Test
	public void testGetProperty() {
		// call under test
		String value = config.getProperty(sampleKey);
		assertEquals(sampleValue, value);
	}
	
	
	@Test (expected=IllegalArgumentException.class)
	public void testGetPropertyDoesNotExist() {
		// call under test
		config.getProperty("doesNotExist");
	}

	@Test
	public void testLogConfiguration() {
		props.put(MigrationConfigurationImpl.KEY_STACK, "dev");
		setupMockSecretManager();
		// call under test
		config.logConfiguration();
		verify(mockLogger, times(8)).info(anyString());
	}


	@Test
	public void testRemainInReadOnlyAfterMigrationDeafult() {
		// by default should return false.
		assertFalse(config.remainInReadOnlyAfterMigration());
	}
	
	@Test
	public void testRemainInReadOnlyAfterMigrationSet() {
		// set the value
		props.put(MigrationConfigurationImpl.KEY_REMAIN_READ_ONLY_MODE, "true");
		assertTrue(config.remainInReadOnlyAfterMigration());
	}

	@Test
	public void testGetConnectionInfoProd() {
		setupMockSecretManager();
		props.put(MigrationConfigurationImpl.KEY_STACK, "prod");
		// source
		SynapseConnectionInfo connInfo = config.getSourceConnectionInfo();
		assertNotNull(connInfo);
		assertEquals("repo-prod.prod.sagebase.org/repo/v1", connInfo.getRepositoryEndPoint());
		assertEquals("repo-prod.prod.sagebase.org/auth/v1", connInfo.getAuthenticationEndPoint());
		assertEquals(serviceKey, connInfo.getServiceKey());
		assertEquals(sourceServiceSecret, connInfo.getServiceSecret());
		// destination
		connInfo = config.getDestinationConnectionInfo();
		assertNotNull(connInfo);
		assertEquals("repo-staging.prod.sagebase.org/repo/v1", connInfo.getRepositoryEndPoint());
		assertEquals("repo-staging.prod.sagebase.org/auth/v1", connInfo.getAuthenticationEndPoint());
		assertEquals(serviceKey, connInfo.getServiceKey());
		assertEquals(destinationServiceSecret, connInfo.getServiceSecret());
	}

	@Test
	public void testGetConnectionInfoDev() {
		props.put(MigrationConfigurationImpl.KEY_STACK, "dev");
		setupMockSecretManager();
		SynapseConnectionInfo connInfo = config.getSourceConnectionInfo();
		assertNotNull(connInfo);
		assertEquals("repo-prod.dev.sagebase.org/repo/v1", connInfo.getRepositoryEndPoint());
		assertEquals("repo-prod.dev.sagebase.org/auth/v1", connInfo.getAuthenticationEndPoint());
		assertEquals(serviceKey, connInfo.getServiceKey());
		assertEquals(sourceServiceSecret, connInfo.getServiceSecret());
		// destination
		connInfo = config.getDestinationConnectionInfo();
		assertNotNull(connInfo);
		assertEquals("repo-staging.dev.sagebase.org/repo/v1", connInfo.getRepositoryEndPoint());
		assertEquals("repo-staging.dev.sagebase.org/auth/v1", connInfo.getAuthenticationEndPoint());
		assertEquals(serviceKey, connInfo.getServiceKey());
		assertEquals(destinationServiceSecret, connInfo.getServiceSecret());
	}

}
