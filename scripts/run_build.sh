#!/usr/bin/env bash

# Copyright 2021 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Disclaimer
# This script is for training purposes only and is to be used only
# in support of approved training. The author assumes no liability
# for use outside of a training environments. Unless required by
# applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, either express or implied.

# Title: run_build.sh
# Author: WKD
# Date: 07APR23
# Purpose: Build script for MkDocs builds of HTML and PDF.

# DEBUG
#set -x
#set -eu
#set >> /tmp/setvar.txt

# VARIABLE
num_arg=$#
dir=${HOME}
option=$1
repo_name=$2
repo_dir=${HOME}/Source/wmdailey/${repo_name}
list_dir=list_dir.txt
#logfile=${dir}/log/$(basename $0).log

# FUNCTIONS
function usage() {
        echo "Usage: $(basename $0) [OPTION]"
        exit
}

function get_help() {
# Help page

cat << EOF
SYNOPSIS
        setup_training.sh [OPTION]

DESCRIPTION
        Setup the student for the user training

        -h, --help
                Help page
	-d, --delete
		Delete student guide directories
        -e, --exercise <repo_name>
		Build PDF for exercises                
        -g, --guide 
		Build HTML for student guide 
        -l, --lecture <repo_name>
		Build PDF for lectures 
	-m, --make
		Make student guide directories
	-o, --out
		Output a file listing student guide directories
EOF
        exit
}

function check_arg() {
# Check if arguments exits

        if [ ${num_arg} -ne "$1" ]; then
                usage
        fi
}

function run_requirement() {

	python3 -m pip install -r requirements.txt
}

function make_dir() {
# Create the directories for lectures and exercises

        while IFS=: read -r new_dir; do
                echo "Adding directory for ${new_dir}"
                mkdir ../student/guide/${new_dir}
        done < ${list_dir}
}

function delete_dir() {
# Create the directories for lectures and exercises

        while IFS=: read -r new_dir; do
                echo "Deleting the directory for ${new_dir}"
                rm -r -f ../student/guide/${new_dir}
        done < ${list_dir}
}

function out_dir() {
# List all of the directories and files into a file.

        ls -R ../student/guide > course_files.txt
	echo "Output to file course_files.txt"
}

function build_guide() {
# Build HTML for student guide

	SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
	cd $SCRIPT_DIR

	echo "This script builds the course student guide."
	mkdocs build
	mkdocs serve
}

function build_exercise() {
# Run this command to create exercise pdf

	echo "This script builds the course exercise pdf document."
	mkdocs build -f exercise.mkdocs.yml

	pdf_doc=${repo_dir}/build/exercise/pdf/document.pdf

	if [ -f ${pdf_doc} ]; then	
		sleep 3
		mv ${pdf_doc} ${HOME}/Desktop/${repo_name}_exercise.pdf
		echo "open ${HOME}/Desktop/${repo_name}_exercise.pdf"
		echo "Use Adobe Acrobat to compress the pdf document before submitting to the LMS."
	else
		echo "The repo directory name does not match. The pdf file is located in <repo>/build/exercise/pdf/document.pdf"			
		echo "Warning: Do not leave the pdf documents in the repo, these files are two large for a git transfer."
	fi
}

function build_lecture() {
# Run this command to create exercise pdf
	
	echo "This script builds the course lecture pdf document."
	mkdocs build -f lecture.mkdocs.yml

	pdf_doc=${repo_dir}/build/lecture/pdf/document.pdf
	echo $pdf_doc

	if [ -f ${pdf_doc} ]; then	
		sleep 3
		mv ${pdf_doc} ${HOME}/Desktop/${repo_name}_lecture.pdf
		echo "open ${HOME}/Desktop/${repo_name}_lecture.pdf"
		echo "Use Adobe Acrobat to compress the pdf document before submitting to the LMS."
	else
		echo "The repo directory name does not match. The pdf file is located in <repo>/build/lecture/pdf/document.pdf"			
		echo "Warning: Do not leave the pdf documents in the repo, these files are two large for a git transfer."
	fi
}

function run_option() {
# Case statement for options.

        case "${option}" in
                -h | --help)
                        get_help
                        ;;
		-d | --delete)
			check_arg 1
			delete_dir
			;;
                -e | --exercise)
                        check_arg 2
			run_requirement
                        build_exercise
                        ;;
                -g |--guide)
                        check_arg 1
			run_requirement
			build_guide
                        ;;
                -l | --lecture)
                        check_arg 2
			run_requirement
                        build_lecture
                        ;;
		-m | --make)
			check_arg 1
			make_dir
			;;
		-o | --out)
			check_arg 1
			out_dir
			;;
                *)
                        usage
                        ;;
        esac
}

function main() {

        # Run command
        run_option

        # Review log file
        #echo "Review log file at ${logfile}"
}

#MAIN
main "$@"
exit 0
