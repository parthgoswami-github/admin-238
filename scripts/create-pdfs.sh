#!/bin/bash

SLIDE_DIR="../slides"
PDF_DIR='../build/pdfs'

mkdir -p $PDF_DIR
rm -rf $PDF_DIR/*

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd $SCRIPT_DIR

which -s pdftk || \
  cat <<- END

  You need PDF Toolkit Server in your path (pdftk).

  You can get it here:
  https://www.pdflabs.com/tools/pdftk-server/

END

cat <<- END

  The script will now use Apple's Javascript for Automation to convert each
  PowerPoint file to PDF.  This is a very fragile process that is likely to
  break each time PowerPoint is updated.  If you need to fix the script, use
  the Script Editor and Accessibility Inspector applications.  Also, be sure
  to check the comments in the script to compare the version of PowerPoint you
  have to the version that the script was created against.

  Note: The script does not change export and print settings.  You should
  export one file (both with notes and without) before running the script
  so PowerPoint remembers the settings.

  DO NOT touch your keyboard or mouse until the process is finished.

END

for FILE in $SLIDE_DIR/*.pptx
do
  osascript -l JavaScript create-note-pdfs.js "$SLIDE_DIR/$FILE" || exit -1
done

echo "Combining files..."
pdftk $SLIDE_DIR/*-instructor.pdf cat output $PDF_DIR/instructor-slides.pdf
pdftk $SLIDE_DIR/*-student.pdf cat output $PDF_DIR/student-slides.pdf

echo "Cleaning up..."
rm $SLIDE_DIR/*-rubbish.pdf  # These are created because of a bug in PowerPoint
rm $SLIDE_DIR/*-instructor.pdf
rm $SLIDE_DIR/*-student.pdf

echo "Done."
