#!/usr/bin/env bash
#
# create a per function link to a single node_modules folder to save space a
# and don't slow down intellij (otherwise a huge number of files are required)
#
function createLinks
{
  TARGET_FOLDER=$1
  for dir in ${TARGET_FOLDER}/*
  do
    cd "${dir}" || exit 1

    if [[ -e node_modules ]]
    then
      echo "skipping creation of link to ../../node_modules in ${dir}"
    else
      echo "creating link to ../../node_modules in ${dir}"
      ln -s ../../node_modules
    fi

    cd -  || exit 1

  done
}


createLinks "RCQ"
createLinks "RQ"



