package org.sagebionetworks.migration.simulation;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.MigrationClient;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

@RunWith(MockitoJUnitRunner.class)
public class SimulatedMigrationIntegrationTest {

	@Test
	public void testMigrateWithHighWaterMark() {
		// source
		SimulatedStack sourceStack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(MigrationType.PRINCIPAL).setMinid(12L).setMaxid(42L),
						new MigrationTypeCount().setType(MigrationType.CHANGE).setMinid(1L).setMaxid(101L)));
		// destination
		SimulatedStack destinationStack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(MigrationType.PRINCIPAL).setMinid(12L).setMaxid(25L),
						new MigrationTypeCount().setType(MigrationType.CHANGE).setMinid(1L).setMaxid(50L)));
		StackSimulator simulator = new StackSimulator(sourceStack, destinationStack);
		MigrationClient client = simulator.createClientWithSimulatedServices();
		// call under test
		client.migrate();
	}

}
