package org.sagebionetworks.migration.utils;

import java.util.Objects;

import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.util.ValidateArgument;

/**
 * An immutable object that captures the state of both the source and
 * destination for a single {@link MigrationType}.
 * 
 */
public class TypeToMigrateMetadata {

	private final MigrationType type;
	private final Long srcMinId;
	private final Long srcMaxId;
	private final Long destMinId;
	private final Long destMaxId;
	private final boolean isSourceReadOnly;

	public TypeToMigrateMetadata(boolean isSourceReadOnly, MigrationTypeCount source, MigrationTypeCount dest) {
		ValidateArgument.required(source, "source");
		ValidateArgument.required(source.getType(), "source.type");
		ValidateArgument.required(dest, "dest");
		if (!source.getType().equals(dest.getType())) {
			throw new IllegalArgumentException(
					String.format("Mismatch type source: '%s' dest: '%s'", source.getType(), dest.getType()));
		}
		this.type = source.getType();
		this.srcMinId = source.getMinid();
		this.srcMaxId = source.getMaxid();
		this.destMinId = dest.getMinid();
		this.destMaxId = dest.getMaxid();
		this.isSourceReadOnly = isSourceReadOnly;
	}

	public MigrationType getType() {
		return type;
	}

	public Long getSrcMinId() {
		return srcMinId;
	}

	public Long getSrcMaxId() {
		return srcMaxId;
	}

	public Long getDestMinId() {
		return destMinId;
	}

	public Long getDestMaxId() {
		return destMaxId;
	}
	
	public static TypeToMigrateMetadataBuilder builder(boolean isSourceReadOnly) {
		return new TypeToMigrateMetadataBuilder(isSourceReadOnly);
	}

	@Override
	public int hashCode() {
		return Objects.hash(destMaxId, destMinId, isSourceReadOnly, srcMaxId, srcMinId, type);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof TypeToMigrateMetadata)) {
			return false;
		}
		TypeToMigrateMetadata other = (TypeToMigrateMetadata) obj;
		return Objects.equals(destMaxId, other.destMaxId) && Objects.equals(destMinId, other.destMinId)
				&& isSourceReadOnly == other.isSourceReadOnly && Objects.equals(srcMaxId, other.srcMaxId)
				&& Objects.equals(srcMinId, other.srcMinId) && type == other.type;
	}

	@Override
	public String toString() {
		return "TypeToMigrateMetadata [type=" + type + ", srcMinId=" + srcMinId + ", srcMaxId=" + srcMaxId
				+ ", destMinId=" + destMinId + ", destMaxId=" + destMaxId + ", isSourceReadOnly=" + isSourceReadOnly
				+ "]";
	}

	public static class TypeToMigrateMetadataBuilder {

		private boolean isSourceReadOnly;
		private MigrationTypeCount source;
		private MigrationTypeCount dest;

		private TypeToMigrateMetadataBuilder(boolean isSourceReadOnly) {
			this.isSourceReadOnly = isSourceReadOnly;
		}

		/**
		 * @param isSourceReadOnly the isSourceReadOnly to set
		 */
		public TypeToMigrateMetadataBuilder setSourceReadOnly(boolean isSourceReadOnly) {
			this.isSourceReadOnly = isSourceReadOnly;
			return this;
		}

		/**
		 * @param source the source to set
		 */
		public TypeToMigrateMetadataBuilder setSource(MigrationTypeCount source) {
			this.source = source;
			return this;
		}

		/**
		 * @param dest the dest to set
		 */
		public TypeToMigrateMetadataBuilder setDest(MigrationTypeCount dest) {
			this.dest = dest;
			return this;
		}

		public TypeToMigrateMetadata build() {
			return new TypeToMigrateMetadata(isSourceReadOnly, source, dest);
		};
	}

}
