#/bin/bash
test "${TRAVIS_PULL_REQUEST}" == "false" \
       && test "${TRAVIS_TAG}" != "" \
       && mvn versions:set -DnewVersion=${TRAVIS_TAG} \
       && mvn deploy --settings travis/bintray-settings.xml ;

