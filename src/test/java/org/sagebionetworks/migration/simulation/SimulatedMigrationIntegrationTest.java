package org.sagebionetworks.migration.simulation;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.MigrationClient;
import static org.sagebionetworks.repo.model.migration.MigrationType.*;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.repo.model.table.Row;

@RunWith(MockitoJUnitRunner.class)
public class SimulatedMigrationIntegrationTest {

	@Test
	public void tesSimpleMigration() {
		// source
		SimulatedStack sourceStack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(12L).setMaxid(42L),
						new MigrationTypeCount().setType(CHANGE).setMinid(1L).setMaxid(101L)));
		// destination
		SimulatedStack destinationStack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(12L).setMaxid(25L),
						new MigrationTypeCount().setType(CHANGE).setMinid(1L).setMaxid(50L)));
		StackSimulator simulator = new StackSimulator(sourceStack, destinationStack);
		MigrationClient client = simulator.createClientWithSimulatedServices();
		// call under test
		client.migrate();

		// the two stacks should be synchronized.
		assertEquals(sourceStack.getRowsOfType(PRINCIPAL), destinationStack.getRowsOfType(PRINCIPAL));
		assertEquals(sourceStack.getRowsOfType(CHANGE), destinationStack.getRowsOfType(CHANGE));
	}

	/**
	 * This is a test for the case where the source stack in in read-write mode and
	 * undergoes changes during migration. For such a case the high-water-mark for
	 * each type must be captured at the beginning of migration and not exceeded
	 * when executing any backup job.
	 */
	@Ignore // See: https://sagebionetworks.jira.com/browse/PLFM-7384
	@Test
	public void tesMigrationWithHighWaterMark() {

		Long sourcePrincipalMaxIdAtStart = 42L;
		Long sourceChangeMaxIdAtStart = 101L;
		// source
		SimulatedStack sourceStack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(12L).setMaxid(sourcePrincipalMaxIdAtStart),
						new MigrationTypeCount().setType(CHANGE).setMinid(1L).setMaxid(sourceChangeMaxIdAtStart)));
		// allow data to change during migration
		sourceStack.setUpdateReadWriteStack(true);
		// destination
		SimulatedStack destinationStack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(12L).setMaxid(25L),
						new MigrationTypeCount().setType(CHANGE).setMinid(1L).setMaxid(50L)));
		StackSimulator simulator = new StackSimulator(sourceStack, destinationStack).withMaximumBackupBatchSize(50);
		MigrationClient client = simulator.createClientWithSimulatedServices();
		// call under test
		client.migrate();

		// See PLFM-7360
		assertEquals(sourcePrincipalMaxIdAtStart,
				destinationStack.getRowsOfType(PRINCIPAL).stream().map(r -> r.getRowId()).max(Long::compareTo).get());
		assertEquals(sourceChangeMaxIdAtStart,
				destinationStack.getRowsOfType(CHANGE).stream().map(r -> r.getRowId()).max(Long::compareTo).get());
	}
	
	/**
	 * With this test the destination has an ID in a bin that is greater than the ID of the maximum ID within the source's bin.
	 * This test simulates the problem found in https://sagebionetworks.jira.com/browse/PLFM-6551.
	 */
	@Test
	public void testMigrationWithDestinationBinnedRangesExceedTheSource() {
		// source
		SimulatedStack sourceStack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(1L).setMaxid(1L)));
		sourceStack.setRowsOfType(PRINCIPAL, List.of(
				new Row().setRowId(0L).setEtag("0"),
				new Row().setRowId(1L).setEtag("1"),
				new Row().setRowId(3L).setEtag("3"),
				new Row().setRowId(4L).setEtag("4")
		));
		
		// destination
		SimulatedStack destinationStack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(1L).setMaxid(1L)));
		destinationStack.setRowsOfType(PRINCIPAL, List.of(
				new Row().setRowId(0L).setEtag("0"),
				new Row().setRowId(1L).setEtag("1"),
				new Row().setRowId(2L).setEtag("2"),
				new Row().setRowId(3L).setEtag("3"),
				new Row().setRowId(4L).setEtag("4"),
				new Row().setRowId(5L).setEtag("5")
		));
		
		// With a batch size of 3 each bin should contain three rows
		StackSimulator simulator = new StackSimulator(sourceStack, destinationStack).withMaximumBackupBatchSize(3);
		MigrationClient client = simulator.createClientWithSimulatedServices();
		// call under test
		client.migrate();
		
		// the two stacks should be synchronized.
		assertEquals(sourceStack.getRowsOfType(PRINCIPAL), destinationStack.getRowsOfType(PRINCIPAL));
	}

}
