#!/usr/local/bin/python3
import io
import re
import sys

# This script will replace all of the numbers that start a line followed by a period with a consistent increasing count.

def convert_numbers(filename):
    count = 1
    numbered_line = re.compile(r"^[0-9]*\.")

    file_in = open(filename, 'rt')
    text_in = file_in.read()
    text_stream_in = io.StringIO(text_in)
    text_stream_out = io.StringIO()

    for line in text_stream_in:
        match = numbered_line.match(line)
        if match:
            text_stream_out.write(str(count) + '.' + line[match.end():])
            count += 1
        else:
            text_stream_out.write(line)

    text_stream_in.close()
    file_in.close()

    file_out = open(filename, 'wt')
    file_out.write(text_stream_out.getvalue())
    file_out.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: convert_numbers.py filename")
        exit(1)
        
    convert_numbers(sys.argv[1])