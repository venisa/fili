#!/bin/bash
PUBLISH_WHITELIST="master foo"

# We only publish on commits to whitelisted branches, not PRs against them or tagging events
# This isn't a pull request build
if [[ "${TRAVIS_PULL_REQUEST}" != "false" ]]; then
    echo "Not publishing, this is PR: ${TRAVIS_PULL_REQUEST}"
    exit 0
fi

if [[ "${TRAVIS_TAG}" != "" ]]; then
    echo "Not publishing, this is a tag event: ${TRAVIS_TAG}"
    exit 0
fi


if [[ ! ${PUBLISH_WHITELIST} =~ ${TRAVIS_BRANCH} ]]; then
    echo "Not publishing, ${TRAVIS_BRANCH} is not in whitelist: ${PUBLISH_WHITELIST}"
    exit 0
fi

echo "Ci tagging: "
travis/ci-tag

echo "Maven versions set : "
mvn versions:set -DnewVersion=$(git describe)

echo "Deploying: "
mvn deploy --settings travis/bintray-settings.xml -DskipTests

echo "Reverting"
mvn versions:revert
