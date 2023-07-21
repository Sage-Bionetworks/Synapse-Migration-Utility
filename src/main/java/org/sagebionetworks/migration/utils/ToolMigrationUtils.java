package org.sagebionetworks.migration.utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

public class ToolMigrationUtils {

	/**
	 * Build the metadata for the primary types to migrateWithRetry
	 *
	 * @param srcCounts
	 * @param destCounts
	 * @param typesToMigrate
	 * @return
	 */
	public static List<TypeToMigrateMetadata> buildTypeToMigrateMetadata(boolean isSourceReadOnly,
		List<MigrationTypeCount> srcCounts, List<MigrationTypeCount> destCounts,
		List<MigrationType> typesToMigrate) {
		if (srcCounts == null) throw new IllegalArgumentException("srcCounts cannot be null.");
		if (destCounts == null) throw new IllegalArgumentException("destCounts cannot be null.");
		if (typesToMigrate == null) throw new IllegalArgumentException("typesToMigrate cannot be null.");
		
		List<TypeToMigrateMetadata> l = new LinkedList<TypeToMigrateMetadata>();
		for (MigrationType t: typesToMigrate) {
			Optional<MigrationTypeCount> srcMtc = srcCounts.stream().filter(c-> t.equals(c.getType())).findFirst();
			if (!srcMtc.isPresent()) {
				throw new RuntimeException("Could not find type " + t.name() + " in source migrationTypeCounts");
			}
			Optional<MigrationTypeCount> destMtc = destCounts.stream().filter(c-> t.equals(c.getType())).findFirst();
			if (!destMtc.isPresent()) {
				throw new RuntimeException("Could not find type " + t.name() + " in destination migrationTypeCounts");
			}
			TypeToMigrateMetadata data = new TypeToMigrateMetadata(isSourceReadOnly, srcMtc.get(), destMtc.get());
			l.add(data);
		}
		return l;
	}

	public static List<MigrationTypeMetaDiff> getMigrationTypeMetaDiffs(List<MigrationTypeCount> srcCounts, List<MigrationTypeCount> destCounts) {
		List<MigrationTypeMetaDiff> result = new LinkedList<MigrationTypeMetaDiff>();
		// All migration types of source should be at destination
		// Note: unused src migration types are covered, they're not in destination results
		for (MigrationTypeCount destMtc: destCounts) {
			MigrationTypeMetaDiff metaDiff = null;
			Optional<MigrationTypeCount> found = srcCounts.stream().filter(mtc->mtc.getType().equals(destMtc.getType())).findFirst();
			if (found.isPresent()) {
				metaDiff = new MigrationTypeMetaDiff(found.get().getType(), new MetaReport(found.get().getMinid(), destMtc.getMinid()), new MetaReport(found.get().getMaxid(), destMtc.getMaxid()));
				result.add(metaDiff);
			}
		}
		return result;
	}

}
