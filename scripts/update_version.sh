#!/bin/bash

version=$GIT_TAG

sed -i "s/VERSION/${GIT_TAG}/g" package.json
