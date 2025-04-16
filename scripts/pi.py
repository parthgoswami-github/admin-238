file_path = "/Users/sanketmote/Documents/GitHub/data_services/exercises/guide/cdw/README.md"

with open(file_path, "r") as file:
    lines = file.readlines()

# Initialize counters for tags
stack_jinja2_comment = []

for i, line in enumerate(lines, start=1):
    if "{#" in line:
        stack_jinja2_comment.append((i, line.strip()))
    if "#}" in line:
        if stack_jinja2_comment:
            stack_jinja2_comment.pop()
        else:
            print(f"Extra closing Jinja2 comment tag '#}}' found on line {i}: {line.strip()}")

# Check for unmatched opening tags
for unmatched in stack_jinja2_comment:
    print(f"Unmatched Jinja2 opening comment tag '{unmatched[1]}' on line {unmatched[0]}")

