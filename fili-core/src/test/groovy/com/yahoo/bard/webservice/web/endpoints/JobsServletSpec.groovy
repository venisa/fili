// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder

import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy

import spock.lang.Specification

import javax.ws.rs.core.Response

/**
 * Test for Jobs endpoint.
 */
class JobsServletSpec extends Specification {
    JerseyTestBinder jtb

    def setup() {
        jtb = new JerseyTestBinder(JobsServlet.class)
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    def "jobs/ticket endpoint returns the correct response to a get request"() {
        setup:
        String expectedResponse = """{
                "query": "https://localhost:9998/v1/data/QUERY",
                "results": "http://localhost:9998/async/ticket1/results",
                "syncResults": "http://localhost:9998/async/ticket1/results?asyncAfter=never",
                "self": "http://localhost:9998/async/ticket1",
                "status": "success",
                "jobTicket": "ticket1",
                "dateCreated": "2016-01-01"
        }"""

        when: "We send a request"
        //The buildApiJobStore method in TestBinderFactory sets up the ApiJobStore and stores the metadata for ticket1 in it.
        String result = makeRequest("/async/ticket1")

        then: "what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "jobs/ticket endpoint returns a 404 if the ticket does not exist in the ApiJobStore"() {
        when:
        Response r = jtb.getHarness().target("/async/IDoNotExist").request().get()

        then:
        r.getStatus() == Response.Status.NOT_FOUND.getStatusCode()
        r.readEntity(String.class) == "No job found with job ticket IDoNotExist"
    }

    def "jobs/result endpoint returns a 404 if the ticket does not exist in the ApiJobStore and the PreResponse is not available in the PreResponseStore before timeout"() {
        when: "We send a request to the async/IDoNotExist/results endpoint"
        Response r = jtb.getHarness().target("/async/IDoNotExist/results").queryParam("asyncAfter", 5).request().get()

        then: "Since the ticket does not exist in the ApiJobStore we get a 404"
        r.getStatus() == Response.Status.NOT_FOUND.getStatusCode()
        r.readEntity(String.class) == "No job found with job ticket IDoNotExist"
    }

    def "jobs/result endpoint returns the result if a ticket does not exist in the ApiJobStore but the PreResponse is available in the PreResponseStore before timeout"() {
        setup:
        String expectedResponse = """{"rows":[{"dateTime":"2016-01-12 00:00:00.000","pageViews":111}]}"""

        when: "We send a request to the async/IExistOnlyInPreResponseStore/results endpoint"
        String result = makeRequest("/async/IExistOnlyInPreResponseStore/results", [asyncAfter : ["5"]])

        then:
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "jobs/result endpoint returns the job metadata if the PreResponse is not available in the PreResponsestore in the async timeout"() {
        setup:
        String expectedResponse = """{
                "query": "https://localhost:9998/v1/data/QUERY",
                "results": "http://localhost:9998/async/ticket2/results",
                "syncResults": "http://localhost:9998/async/ticket2/results?asyncAfter=never",
                "self": "http://localhost:9998/async/ticket2",
                "status": "pending",
                "jobTicket": "ticket2",
                "dateCreated": "2016-01-01"
        }"""

        when: "We send a request to the async/ticket2/results endpoint"
        //The buildApiJobStore method in TestBinderFactory sets up the ApiJobStore and stores the metadata for ticket2 in it.
        String result = makeRequest("/async/ticket2/results", [asyncAfter : ["5"]])

        then: "Since the job is not available in the PreResponseStore before the async timeout we get the job payload back"
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "jobs/result endpoint returns the expected response when the PreResponse is available in the PreResponseStore before async timeout"() {
        setup:
        String expectedResponse = """{"rows":[{"dateTime":"2016-01-12 00:00:00.000","pageViews":111}]}"""

        when: "We send a request to the async/ticket1/results endpoint"
        String result = makeRequest("/async/ticket1/results", [asyncAfter : ["5"]])

        then:
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "jobs/result endpoint returns an error if the PreResponse contains an error"() {
        setup:
        String expectedResponse = """{
          "status" : 500,
          "statusName" : "Internal Server Error",
          "reason" : "Error",
          "description" : "Error",
          "druidQuery" : null
        }"""

        when: "We send a request to the async/errorPreResponse/results endpoint"
        Response r = jtb.getHarness().target("/async/errorPreResponse/results").request().get()

        then:
        r.getStatus() == 500
        GroovyTestUtils.compareJson(r.readEntity(String.class), expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "/jobs endpoint returns the payload for all the jobs in the ApiJobStore"() {
        setup:
        String expectedResponse = """{"async":[
                                        {
                                            "dateCreated":"2016-01-01",
                                            "jobTicket":"ticket1",
                                            "query":"https://localhost:9998/v1/data/QUERY",
                                            "results":"http://localhost:9998/async/ticket1/results",
                                            "self":"http://localhost:9998/async/ticket1",
                                            "status":"success",
                                            "syncResults":"http://localhost:9998/async/ticket1/results?asyncAfter=never"
                                        },
                                        {
                                            "dateCreated":"2016-01-01",
                                            "jobTicket":"ticket2",
                                            "query":"https://localhost:9998/v1/data/QUERY",
                                            "results":"http://localhost:9998/async/ticket2/results",
                                            "self":"http://localhost:9998/async/ticket2",
                                            "status":"pending",
                                            "syncResults":"http://localhost:9998/async/ticket2/results?asyncAfter=never"
                                        }
                                  ]}"""

        when: "We send a request to the async endpoint"
        //The buildApiJobStore method in TestBinderFactory sets up the ApiJobStore and stores the metadata for tickets in it.
        String result = makeRequest("/async")

        then: "what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    String makeRequest(String target) {
        makeRequest(target, [:])
    }

    String makeRequest(String target, Map<String, List<String>> queryParams ) {
        // Set target of call
        def httpCall = jtb.getHarness().target(target)

        // Add query params to call
        queryParams.each { String key, List<String> values ->
            httpCall = httpCall.queryParam(key, values.join(","))
        }

        // Make the call
        httpCall.request().get(String.class)
    }
}
