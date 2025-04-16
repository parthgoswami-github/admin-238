#!/usr/bin/env bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd $SCRIPT_DIR

echo "This script builds the course content."

export GIT_VERSION=`git describe --all --long | cut -d "-" -f 3`

rm -rf ../build
mkdir -p ../build/tmp

python3 -m pip install -r requirements.txt --progress-bar off

# Build the student exercise guide
mkdocs build
cp -R ../exercises/guide/stylesheets ../build/AMP/exercises/
cp -r ../build/AMP/exercises/assets/* ../build/AMP/exercises/guide/assets/
# Build the instructor AMP - which includes the student exercise guide
echo "Building the instructor AMP"
mkdir -p ../build/AMP
cp ../exercises/README.md ../build/AMP/
cp ../exercises/.project-metadata.yaml ../build/AMP
cp ../exercises/requirements.txt ../build/AMP
cp -R ../exercises/cml ../build/AMP
cp -R ../exercises/data ../build/S3

cd ../build/AMP
zip -rq ../amp.zip * .project-metadata.yaml
cd $SCRIPT_DIR

## Build Training_Definitive content
mkdir -p ../build/training-definitive

# Build exercise guide
echo "Building the exercise guide"
cd ../build/AMP/exercises
zip -rq  ../../../build/training-definitive/exercise-guide.zip ./* -x 'data/*' 'cml/*' 'code/*'
cd $SCRIPT_DIR

# Build PDFs
echo "Building slides/PDFs -- Need to uncomment if you want build the PDFs"
# ./create-pdfs.sh

## Build git repos
echo "Building git repos"

#  edu-cml-on-cdp-evidently
mkdir -p ../build/edu-cml-on-cdp-evidently
cp -R ../exercises/guide/03-02-model_monitoring/code ../build/edu-cml-on-cdp-evidently/
cp -R ../exercises/guide/03-02-model_monitoring/cml ../build/edu-cml-on-cdp-evidently/
cp ../exercises/guide/03-02-model_monitoring/.project-metadata.yaml ../build/edu-cml-on-cdp-evidently/
cat > '../build/edu-cml-on-cdp-evidently/README.md' <<EOD
# CML on CDP - Evidently

This project contains a Jupyter notebook to demonstrate Evidently.

VERSION: ${GIT_VERSION}
EOD

# Clean up
# echo "Cleaning up"
# rm -rf ../build/AMP
# rm -rf ../build/AMP-Student
# rm -rf ../build/instructor-guide
# rm -rf ../build/tmp


# mkdocs serve