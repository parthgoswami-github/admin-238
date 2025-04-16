#!/bin/bash

rm regression.py
mkdir -p tmp
cp ../code/python/* tmp/
cp ../code/python/solutions/* tmp/
cat tmp/*.py > regression.py
rm tmp/*