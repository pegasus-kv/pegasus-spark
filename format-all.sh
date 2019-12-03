#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd "${PROJECT_DIR}" || exit 1

SRC_FILES=(pegasus-core/src/main/java/com/xiaomi/infra/pegasus/spark/core/*.java
           pegasus-bulkloader/src/main/java/com/xiaomi/infra/pegasus/spark/bulkloader/*.java
           pegasus-analyser/src/main/java/com/xiaomi/infra/pegasus/spark/analyser/*.java
           )

if [ ! -f "${PROJECT_DIR}"/google-java-format-1.7-all-deps.jar ]; then
    wget https://github.com/google/google-java-format/releases/download/google-java-format-1.7/google-java-format-1.7-all-deps.jar
fi
java -jar "${PROJECT_DIR}"/google-java-format-1.7-all-deps.jar --replace "${SRC_FILES[@]}"
