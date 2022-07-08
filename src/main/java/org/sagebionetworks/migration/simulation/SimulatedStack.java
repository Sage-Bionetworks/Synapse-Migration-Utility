package org.sagebionetworks.migration.simulation;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousAdminRequestBody;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRequest;
import org.sagebionetworks.repo.model.migration.AsyncMigrationResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationTypeCountsRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;
import org.sagebionetworks.repo.model.migration.BatchChecksumRequest;
import org.sagebionetworks.repo.model.migration.BatchChecksumResponse;
import org.sagebionetworks.repo.model.migration.CalculateOptimalRangeRequest;
import org.sagebionetworks.repo.model.migration.CalculateOptimalRangeResponse;
import org.sagebionetworks.repo.model.migration.IdRange;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.repo.model.migration.MigrationTypeCounts;
import org.sagebionetworks.repo.model.migration.MigrationTypeNames;
import org.sagebionetworks.repo.model.migration.RangeChecksum;
import org.sagebionetworks.repo.model.migration.RestoreTypeRequest;
import org.sagebionetworks.repo.model.migration.RestoreTypeResponse;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.schema.adapter.org.json.EntityFactory;

public class SimulatedStack {

	private StackStatus statckStatus;
	private SimulatedStack thisObject;
	private Map<MigrationType, List<Row>> typeToRows;
	private long jobIdCounter;
	private Map<String, AsyncMigrationRequest> requestJobs;
	private boolean updateReadWriteStack = false;

	/**
	 * Create a new simulated stack with the configured data.
	 * 
	 * @param stackData
	 */
	public SimulatedStack(List<MigrationTypeCount> stackData) {
		thisObject = this;
		statckStatus = new StackStatus().setStatus(StatusEnum.READ_WRITE);
		jobIdCounter = 0;
		requestJobs = new LinkedHashMap<String, AsyncMigrationRequest>();
		updateReadWriteStack = false;
		buildRowsForEachType(stackData);
	}
	
	

	/**
	 * @return the updateReadWriteStack
	 */
	public boolean isUpdateReadWriteStack() {
		return updateReadWriteStack;
	}



	/**
	 * @param updateReadWriteStack the updateReadWriteStack to set
	 */
	public void setUpdateReadWriteStack(boolean updateReadWriteStack) {
		this.updateReadWriteStack = updateReadWriteStack;
	}



	/**
	 * Get the actual rows for the given type.
	 * 
	 * @param type
	 * @return
	 */
	public List<Row> getRowsOfType(MigrationType type) {
		return typeToRows.get(type);
	}
	
	/**
	 * Override the rows for a given type.
	 * @param type
	 * @param rows
	 */
	public void setRowsOfType(MigrationType type, List<Row> rows) {
		typeToRows.put(type, new ArrayList<Row>(rows));
	}

	/**
	 * For each type, create rowsIds to represent each row for each type.
	 * 
	 * @param stackData
	 */
	void buildRowsForEachType(List<MigrationTypeCount> stackData) {
		typeToRows = new LinkedHashMap<>(stackData.size());
		for (MigrationTypeCount typeCount : stackData) {
			List<Row> rowIds = new ArrayList<>((int) (typeCount.getMaxid() - typeCount.getMinid()));
			for (Long i = typeCount.getMinid(); i <= typeCount.getMaxid(); i++) {
				rowIds.add(new Row().setRowId(i).setEtag(UUID.randomUUID().toString()));
			}
			typeToRows.put(typeCount.getType(), rowIds);
		}
	}

	/**
	 * Add a new row of the provide type and rowId.
	 * 
	 * @param type
	 * @param rowId
	 * @return
	 */
	public Row addRow(MigrationType type, Long rowId) {
		Row row = new Row().setRowId(rowId).setEtag(UUID.randomUUID().toString());
		typeToRows.get(type).add(row);
		return row;
	}

	public StackStatus getCurrentStackStatus() {
		return statckStatus;
	}

	public StackStatus updateCurrentStackStatus(StackStatus updated) throws SynapseException {
		this.statckStatus = updated;
		return this.statckStatus;
	};

	public MigrationTypeNames getMigrationTypeNames() throws SynapseException {
		return new MigrationTypeNames()
				.setList(typeToRows.keySet().stream().map(MigrationType::name).collect(Collectors.toList()));
	}

	public MigrationTypeNames getPrimaryTypeNames() throws SynapseException {
		return getMigrationTypeNames();
	}

	public AsynchronousJobStatus startAdminAsynchronousJob(AsynchronousAdminRequestBody request)
			throws SynapseException {
		String jobId = Long.toString(jobIdCounter++);
		requestJobs.put(jobId, (AsyncMigrationRequest) request);
		return new AsynchronousJobStatus().setJobId(jobId).setRequestBody(request)
				.setJobState(AsynchJobState.PROCESSING).setStartedOn(new Date());
	}

	/**
	 * Get the status of an Asynchronous Job from its ID.
	 * 
	 * @param jobId
	 * @return
	 * @throws SynapseException
	 */
	public AsynchronousJobStatus getAdminAsynchronousJobStatus(String jobId) throws SynapseException {
		// execute the job on start
		AsyncMigrationRequest request = requestJobs.get(jobId);
		if (request == null) {
			throw new IllegalArgumentException("Cannot find job: " + jobId);
		}
		return new AsynchronousJobStatus().setJobId(jobId).setJobState(AsynchJobState.COMPLETE).setRequestBody(request)
				.setStartedOn(Date.from(Instant.now().minus(1, ChronoUnit.SECONDS))).setResponseBody(
						new AsyncMigrationResponse().setAdminResponse(executeRequest(request.getAdminRequest())));
	}

	private AdminResponse executeRequest(AdminRequest request) {
		try {
			String methodName = "execute" + request.getClass().getSimpleName();
			Method method = thisObject.getClass().getMethod(methodName, request.getClass());
			AdminResponse response = (AdminResponse) method.invoke(thisObject, request);
			// change the stack if needed.
			if(updateReadWriteStack && StatusEnum.READ_WRITE.equals(statckStatus.getStatus())) {
				deleteUpdateAndAddRowForEachType();
			}
			return response;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Delete, update and add a row for each type.
	 * 
	 */
	public void deleteUpdateAndAddRowForEachType() {
		thisObject.typeToRows.forEach((t,r) ->{
			// delete the middle row
			r.remove(r.size()/2);
			// update the new middle row
			r.get(r.size()/2).setEtag(UUID.randomUUID().toString());
			// add a new row
			long maxRowId = r.stream().map(p->p.getRowId()).max(Long::compareTo).get();
			r.add(new Row().setRowId(maxRowId+1).setEtag(UUID.randomUUID().toString()));
		});
	}

	public MigrationTypeCounts executeAsyncMigrationTypeCountsRequest(AsyncMigrationTypeCountsRequest request) {
		List<MigrationTypeCount> list = new ArrayList<>(typeToRows.size());
		typeToRows.forEach((k, v) -> {
			long min = Long.MAX_VALUE;
			long max = Long.MIN_VALUE;
			long count = 0;
			for (Row row : v) {
				min = Math.min(min, row.getRowId());
				max = Math.max(max, row.getRowId());
				count++;
			}
			list.add(new MigrationTypeCount().setType(k).setCount(count).setMinid(min).setMaxid(max));
		});
		return new MigrationTypeCounts().setList(list);
	}

	/**
	 * @param request
	 * @return
	 */
	public CalculateOptimalRangeResponse executeCalculateOptimalRangeRequest(CalculateOptimalRangeRequest request) {
		List<Row> rows = typeToRows.get(request.getMigrationType());
		List<IdRange> results = new ArrayList<>();
		IdRange current = null;
		int counter = 0;
		for (Row row : rows) {
			if (row.getRowId() >= request.getMinimumId() && row.getRowId() <= request.getMaximumId()) {
				if (counter >= request.getOptimalRowsPerRange() + 1) {
					current = null;
					counter = 0;
				}
				if (current == null) {
					current = new IdRange().setMinimumId(row.getRowId()).setMaximumId(row.getRowId());
					results.add(current);
				} else {
					current.setMinimumId(Math.min(current.getMinimumId(), row.getRowId()));
					current.setMaximumId(Math.max(current.getMaximumId(), row.getRowId()));
				}
				counter++;
			}
		}
		return new CalculateOptimalRangeResponse().setMigrationType(request.getMigrationType()).setRanges(results);
	}

	/**
	 * Write the requested data to a backup file.
	 * 
	 * @param request
	 * @return
	 */
	public BackupTypeResponse executeBackupTypeRangeRequest(BackupTypeRangeRequest request) {
		try {
			Path temp = Files.createTempFile("MigrationBackup", ".json");
			List<Row> rows = typeToRows.get(request.getMigrationType());
			JSONObject json = new JSONObject();
			json.put("minId", request.getMinimumId());
			json.put("maxId", request.getMaximumId());
			json.put("type", request.getMigrationType().name());
			JSONArray array = new JSONArray();
			for (Row row : rows) {
				if (row.getRowId() >= request.getMinimumId() && row.getRowId() <= request.getMaximumId()) {
					array.put(EntityFactory.createJSONObjectForEntity(row));
				}
			}
			json.put("rows", array);
			try (Writer writer = new FileWriter(temp.toFile(), StandardCharsets.UTF_8)) {
				IOUtils.write(json.toString(), writer);
			}
			return new BackupTypeResponse().setBackupFileKey(temp.toString());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Restore all of the data from the backup file.
	 * 
	 * @param request
	 * @return
	 */
	public RestoreTypeResponse executeRestoreTypeRequest(RestoreTypeRequest request) {

		List<Row> rowsFromSource = readAndDeleteBackupFile(request);

		List<Row> existingRows = typeToRows.get(request.getMigrationType());
		// first delete each row within the range

		Iterator<Row> it = existingRows.iterator();
		while (it.hasNext()) {
			Row existingRow = it.next();
			if (existingRow.getRowId() >= request.getMinimumRowId()
					&& existingRow.getRowId() <= request.getMaximumRowId()) {
				it.remove();
			}
		}
		// add all of the new rows
		existingRows.addAll(rowsFromSource);
		// sort by rowId
		Collections.sort(existingRows, (Row o1, Row o2) -> o1.getRowId().compareTo(o2.getRowId()));
		return new RestoreTypeResponse().setRestoredRowCount(Integer.valueOf(rowsFromSource.size()).longValue());
	}

	/**
	 * Helper to read the contents of a backup file.
	 * 
	 * @param request
	 * @return
	 */
	public List<Row> readAndDeleteBackupFile(RestoreTypeRequest request) {
		try {
			File temp = new File(request.getBackupFileKey());
			try (Reader reader = new FileReader(temp, StandardCharsets.UTF_8)) {
				JSONObject backup = new JSONObject(IOUtils.toString(reader));

				Long maxId = backup.getLong("maxId");
				if (!maxId.equals(request.getMaximumRowId())) {
					throw new IllegalStateException("MaxId does not match");
				}
				Long minId = backup.getLong("minId");
				if (!minId.equals(request.getMinimumRowId())) {
					throw new IllegalStateException("MinId does not match");
				}
				MigrationType type = MigrationType.valueOf(backup.getString("type"));
				if (!type.equals(request.getMigrationType())) {
					throw new IllegalStateException("type does not match");
				}
				JSONArray array = backup.getJSONArray("rows");
				List<Row> rows = new ArrayList<>(array.length());
				for (int i = 0; i < array.length(); i++) {
					rows.add(EntityFactory.createEntityFromJSONObject(array.getJSONObject(i), Row.class));
				}
				return rows;
			} finally {
				temp.delete();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public BatchChecksumResponse executeBatchChecksumRequest(BatchChecksumRequest request) {
		List<Row> rows = typeToRows.get(request.getMigrationType());
		Map<Long, List<Row>> binToRows = new LinkedHashMap<>(rows.size());

		for (Row row : rows) {
			if (row.getRowId() >= request.getMinimumId() && row.getRowId() <= request.getMaximumId()) {
				Long bin = row.getRowId() / request.getBatchSize();
				List<Row> list = binToRows.get(bin);
				if(list == null) {
					list = new ArrayList<Row>();
					binToRows.put(bin, list);
				}
				list.add(row);
			}
		}
		List<RangeChecksum> resutls = new ArrayList<>();

		binToRows.forEach((b, r) -> {
			CRC32 crc = new CRC32();
			long binNumber = b;
			long count = 0L;
			long minimumId = Long.MAX_VALUE;
			long maximumId = Long.MIN_VALUE;
			for (Row row : r) {
				count++;
				minimumId = Math.min(minimumId, row.getRowId());
				maximumId = Math.max(maximumId, row.getRowId());
				crc.update(new StringJoiner("-").add(row.getRowId().toString()).add(row.getEtag())
						.add(request.getSalt()).toString().getBytes(StandardCharsets.UTF_8));
			}
			resutls.add(new RangeChecksum().setBinNumber(binNumber).setCount(count).setMinimumId(minimumId)
					.setMaximumId(maximumId).setChecksum("" + crc.getValue()));

		});
		return new BatchChecksumResponse().setCheksums(resutls).setMigrationType(request.getMigrationType());
	}

	/**
	 * Simulate a stack by providing a {@link SynapseAdminClient} implementation.
	 * The resulting implementation is a proxy to this object.
	 * 
	 * @return
	 */
	public SynapseAdminClient createProxy() {
		return (SynapseAdminClient) Proxy.newProxyInstance(SimulatedStack.class.getClassLoader(),
				new Class[] { SynapseAdminClient.class }, (proxy, method, methodArgs) -> {
					Method outerMethod = SimulatedStack.class.getMethod(method.getName(), method.getParameterTypes());
					return outerMethod.invoke(thisObject, methodArgs);
				});
	}

}
