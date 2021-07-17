
import os
import re
import sys
from pathlib import Path


def create_file(file):
    with open(file, 'w'):
        pass


def remove_file(file):
    if os.path.exists(file):
        os.remove(file)


def main():

    input_file = sys.argv[1]
    batch_size = int(sys.argv[2])

    work_dir = os.path.expanduser('~/bda-cw-workdir')
    failed_lines_file_path = os.path.join(work_dir, "failed.data")
    output_file_path = os.path.join(work_dir, "clean.data")

    Path(work_dir).mkdir(parents=False, exist_ok=True)

    remove_file(output_file_path)
    remove_file(failed_lines_file_path)

    create_file(output_file_path)

    print("Work directory setup completed.")

    pattern = regex()

    # dry_run_limit = 10000
    flush = 0
    total_flushed = 0
    batch = []
    approximate_lines = 10365152  # Approximation only
    failed_lines = []

    total_lines = 0
    total_matched = 0
    total_failed = 0

    print("Proceeding to read file...")
    f = open(input_file)
    line = f.readline()
    while line:
        # if (total_lines == dry_run_limit):
        #     break
        total_lines += 1
        flush += 1
        if bool(re.match(pattern, line)):
            total_matched += 1
            matches = re.search(pattern, line)
            row_contents = [
                matches.group('remote_addr'),
                matches.group('remote_user'),
                matches.group('time_local'),
                matches.group('method'),
                matches.group('resource'),
                matches.group('http_version'),
                matches.group('status'),
                matches.group('body_bytes_sent'),
                matches.group('http_ref'),
                matches.group('user_agent'),
            ]
            batch.append(row_contents)
            if flush == batch_size:
                for l in batch:
                    append_to_file(output_file_path, l)
                total_flushed += batch_size
                as_per = (total_flushed * 100) / approximate_lines
                print(
                    f'Progress {"{:.2f}".format(as_per)}% {total_flushed}', end="\r")
                flush = 0
                batch = []
        else:
            total_failed += 1
            failed_lines.append(line)

        # Next iteratee
        line = f.readline()

    # Flush the remaining data.
    print("\nAlmost finished...")
    for l in batch:
        append_to_file(output_file_path, l)

    # Save failed lines for analysis.
    if len(failed_lines) > 0:
        create_file(failed_lines_file_path)
        for l in failed_lines:
            append_to_file(failed_lines_file_path, l)

    print("-----------------------------")
    print(f'Total lines: {total_lines}')
    print(f'Total matched: {total_matched}')
    print(f'Total failed: {total_failed}')


def regex():

    # First construct the regular expression matching schema to normalize the
    # dataset to make it loadable to data processors.

    remote_addr = r'(?P<remote_addr>(?:\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})|(?:(?:(?:(?:[0-9a-fA-F]){1,4})\:){7}(?:[0-9a-fA-F]){1,4}))'
    remote_user = r'(?P<remote_user>\-|.*)'
    time_local = r'(?P<time_local>\-|.*)'
    http_method = r'(?P<method>\-|(?:GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH))'
    resource = r"(?P<resource>\-|.*)"
    http_version = r'(?P<http_version>\-|(?:HTTP/\d?.?\d))'
    status = r'(?P<status>\-|\d{3})'
    body_bs = r'(?P<body_bytes_sent>\-|\d+)'
    http_ref = r"(?P<http_ref>\-|.*)"
    user_agent = r'(?P<user_agent>\-|.*)'

    all_or_nothing = r'(?:\-|.*)'
    space = r'\s{0,3}'
    quo = r'\"'

    return (
        r'^' + space + r'?' + remote_addr + space + all_or_nothing +
        space + remote_user + space +
        r'\[' + time_local + r'\]' + space +
        quo + r'(?:' + http_method + space + resource + space + http_version + r')' + quo +
        space + status + space + body_bs + space +
        quo + http_ref + quo + space +
        quo + user_agent + quo + space +
        quo + all_or_nothing + quo + r'$'
    )


def append_to_file(file_name, list_of_elem):
    with open(file_name, 'a') as file:
        file.write("\t".join(list_of_elem) + "\n")


if __name__ == '__main__':
    main()
