#!/bin/bash

# if the data directory exists then destroy it
if [ -d ./data ]
then
    rm -rf ./data
fi

mkdir data
