package org.sagebionetworks.migration.utils;

import java.util.Objects;

public class MetaReport {

    private Long v1, v2;

    public MetaReport() {}

    public MetaReport(Long v1, Long v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public Long getV1() {
        return v1;
    }

    public void setV1(Long v1) {
        this.v1 = v1;
    }

    public Long getV2() {
        return v2;
    }

    public void setV2(Long v2) {
        this.v2 = v2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaReport that = (MetaReport) o;
        return Objects.equals(v1, that.v1) && Objects.equals(v2, that.v2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(v1, v2);
    }

    @Override
    public String toString() {
        return String.format("(%d, %d)", v1, v2);
    }

    public boolean sourceEqualsDest() {
        if (v1 != null && v2 != null) {
            return v1.equals(v2);
        } else {
            return false;
        }
    }
}
