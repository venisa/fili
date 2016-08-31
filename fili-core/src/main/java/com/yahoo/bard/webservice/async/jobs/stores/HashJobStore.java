// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobField;
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.JOBFIELD_NOT_PRESENT_IN_JOB_META_DATA;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An ApiJobStore backed by an in-memory map. This is meant as a stub implementation for
 * testing and playing purposes. It is _not_ meant to be used in production. For one, it stores the ticket
 * information in memory, which is not durable. For another, it does not attempt to cleanup sufficiently old jobs,
 * so its memory footprint will grow until the system is rebooted.
 */
public class HashJobStore implements ApiJobStore {

    private static final Logger LOG = LoggerFactory.getLogger(HashJobStore.class);

    private final Map<String, JobRow> store;

    /**
     * Builds a job store using the passed in map as the backing store.
     *
     * @param store  The map to use to store job metadata
     */
    public HashJobStore(Map<String, JobRow> store) {
        this.store = store;
    }

    /**
     * Constructs an empty HashJobStore, using a {@link LinkedHashMap} as the backing store.
     */
    public HashJobStore() {
        this(new LinkedHashMap<>());
    }

    @Override
    public Observable<JobRow> get(String id) {
        JobRow jobRow  = store.get(id);
        return jobRow == null ? Observable.empty() : Observable.just(jobRow);
    }

    @Override
    public Observable<JobRow> save(JobRow metadata) {
        store.put(metadata.getId(), metadata);
        return Observable.just(metadata);
    }

    @Override
    public Observable<JobRow> getAllRows() {
        return Observable.from(store.values());
    }

    @Override
    public Observable<JobRow> getFilteredRows(Set<ApiJobStoreFilter> apiJobStoreFilters)
            throws IllegalArgumentException {
        return Observable.from(
                store.entrySet().stream()
                        .filter(entry -> satisfiesFilters(apiJobStoreFilters, entry.getValue()))
                        .map(entry -> entry.getValue())
                        .collect(Collectors.toSet())
        );
    }

    /**
     * This method checks if the given JobRow satisfies all the ApiJobStoreFilters and returns true if it does.
     * If a JobField in any of the filters is not a part the JobRow, this method throws a IllegalArgumentException.
     *
     * @param apiJobStoreFilters  A Set of ApiJobStoreFilters specifying the different conditions to be satisfied
     * @param jobRow  The JobRow which needs to be inspected
     *
     * @return true if the JobRow satisfies all the filters, false otherwise
     *
     * @throws IllegalArgumentException if a JobField in any of the filters is not a part the JobRow
     */
    private boolean satisfiesFilters(Set<ApiJobStoreFilter> apiJobStoreFilters, JobRow jobRow)
            throws IllegalArgumentException {
        for (ApiJobStoreFilter filter : apiJobStoreFilters) {
            JobField jobField = filter.getJobField();
            if (!jobRow.containsKey(jobField)) {
                Set<JobField> jobFields = jobRow.keySet();
                LOG.debug(JOBFIELD_NOT_PRESENT_IN_JOB_META_DATA.logFormat(jobField, jobFields));
                throw new IllegalArgumentException(JOBFIELD_NOT_PRESENT_IN_JOB_META_DATA.format(jobField, jobFields));
            }

            if (!filter.getValues().contains(jobRow.get(jobField))) {
                return false;
            }
        }
        return true;
    }
}
