package org.sagebionetworks.migration.utils;
import org.sagebionetworks.repo.model.migration.MigrationType;
import java.util.Objects;

public class MigrationTypeMetaDiff {
    private MigrationType migrationType;
    private MetaReport minReport, maxReport;

    public MigrationTypeMetaDiff(MigrationType migrationType, MetaReport minReport, MetaReport maxReport) {
        this.migrationType = migrationType;
        this.minReport = minReport;
        this.maxReport = maxReport;
    }

    public MigrationType getMigrationType() {
        return migrationType;
    }

    public void setMigrationType(MigrationType migrationType) {
        this.migrationType = migrationType;
    }

    public MetaReport getMinReport() {
        return minReport;
    }

    public void setMinReport(MetaReport minReport) {
        this.minReport = minReport;
    }

    public MetaReport getMaxReport() {
        return maxReport;
    }

    public void setMaxReport(MetaReport maxReport) {
        this.maxReport = maxReport;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationTypeMetaDiff that = (MigrationTypeMetaDiff) o;
        return migrationType == that.migrationType && Objects.equals(minReport, that.minReport) && Objects.equals(maxReport, that.maxReport);
    }

    @Override
    public int hashCode() {
        return Objects.hash(migrationType, minReport, maxReport);
    }

}
