// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores

import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.JOB_TICKET
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.USER_ID

import com.yahoo.bard.webservice.async.jobs.JobTestUtils
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow
import com.yahoo.bard.webservice.util.ReactiveTestUtils


import rx.observers.TestSubscriber
import spock.lang.Shared
import spock.lang.Unroll

/**
 * Verifies that the HashJobStore satisfies the ApiJobStore interface. The tests may be found in
 * {@link ApiJobStoreSpec}.
 */
class HashJobStoreSpec extends ApiJobStoreSpec {
    @Shared
    HashJobStore hashJobStore
    @Shared
    JobRow userFooJobRow1
    @Shared
    JobRow userFooJobRow2
    @Shared
    JobRow userBarJobRow1
    @Shared
    ApiJobStoreFilter userIdFilter
    @Shared
    ApiJobStoreFilter jobTicketFilter

    @Override
    ApiJobStore getStore() {
        new HashJobStore()
    }

    def childSetupSpec() {
        hashJobStore = new HashJobStore()
        userFooJobRow1 = JobTestUtils.buildJobRow([(JOB_TICKET): "1", (USER_ID): "Foo"])
        userFooJobRow2 = JobTestUtils.buildJobRow([(JOB_TICKET): "2", (USER_ID): "Foo"])
        userBarJobRow1 = JobTestUtils.buildJobRow([(JOB_TICKET): "3", (USER_ID): "Bar"])

        hashJobStore.save(userFooJobRow1).subscribe()
        hashJobStore.save(userFooJobRow2).subscribe()
        hashJobStore.save(userBarJobRow1).subscribe()

        userIdFilter = new ApiJobStoreFilter(USER_ID, ApiJobStoreFilterOperation.eq, ["Foo"] as Set)
        jobTicketFilter = new ApiJobStoreFilter(JOB_TICKET, ApiJobStoreFilterOperation.eq, ["1"] as Set)
    }

    def "The backing map is invoked once per store access regardless of the number of observers subscribed"() {
        given: "A mocked out map, and a store that uses it as the backing data store"
        Map<String, JobRow> mockMap = Mock(Map)
        HashJobStore store = new HashJobStore(mockMap)

        when: "We request data from store, and assign a whole mess of observers to it"
        ReactiveTestUtils.subscribeObservers(store.get("0"), 10)

        and: "save data in the store and assign a whole mess of observers to it"
        ReactiveTestUtils.subscribeObservers(store.save(JobTestUtils.buildJobRow(0)), 10)

        and: "request all the rows in the store and assign a whole mess of observers to it"
        ReactiveTestUtils.subscribeObservers(store.getAllRows(), 10)

        then: "The store's operations are accessed only once"
        1 * mockMap.get(_)
        1 * mockMap.put(_, _)
        1 * mockMap.values() >> ([] as Set)
    }

    @Unroll
    def "getFilteredRows returns #jobRows that satisfy #filters"() {
        setup:
        TestSubscriber<JobRow> testSubscriber = new TestSubscriber<>()

        when:
        hashJobStore.getFilteredRows(filters as Set).subscribe(testSubscriber)

        then:
        testSubscriber.assertReceivedOnNext(jobRows)

        where:
        filters                          | jobRows
        [userIdFilter]                   | [userFooJobRow1, userFooJobRow2]
        [userIdFilter, jobTicketFilter]  | [userFooJobRow1]
    }

    def "getFilteredRows throws IllegalArgumentException if we try to filter on a field not present in the JobRow"() {
        setup:
        HashJobStore hashJobStore = new HashJobStore()
        JobRow jobRowWithoutUserId = new JobRow(JOB_TICKET, [(JOB_TICKET): "1"])
        hashJobStore.save(jobRowWithoutUserId)

        ApiJobStoreFilter apiJobStoreFilter = new ApiJobStoreFilter(USER_ID, ApiJobStoreFilterOperation.eq, ["Foo"] as Set)

        when:
        hashJobStore.getFilteredRows([apiJobStoreFilter] as Set)

        then:
        IllegalArgumentException exception = thrown()
        exception.message == "JobField 'userId' is not a part of job meta data. The possible fields to filter on are '[jobTicket]'"
    }
}
