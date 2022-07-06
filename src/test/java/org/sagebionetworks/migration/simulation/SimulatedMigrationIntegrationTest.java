package org.sagebionetworks.migration.simulation;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.MigrationClient;
import static org.sagebionetworks.repo.model.migration.MigrationType.*;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

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

	@Test
	public void tesMigrationSourceInReadWrite() {

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

}
