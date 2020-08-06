#!/usr/bin/env bash
#
# create a per function link to a single node_modules folder to save space a
# and don't slow down intellij (otherwise a huge number of files are required)
#
function copyCommon
{
  TARGET_FOLDER=$1
  for dir in "${TARGET_FOLDER}"/*
  do
    cd "${dir}" || exit 1
    rm common.js common_firestore.js common_firebase.js common_mysql.js
    cp ../../common.js ../../common_firestore.js ../../common_firebase.js ../../common_mysql.js .
    cd -  || exit 1

  done
}


copyCommon "RCQ"
copyCommon "RQ"



