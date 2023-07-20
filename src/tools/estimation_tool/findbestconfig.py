import paramiko

import sys
import time
import json
import subprocess
import threading
import getpass
import csv
import heapq
import signal


CHUNK_ALGORITMH_OPTIONS = ['fixed', 'fastcdc']
CHUNK_SIZE_OPTIONS = ['65536', '32768', '16384', '8192', '4096']
RESULT_FILE = 'result.txt'
WINDOW_BITS =  '2'
FP_ALGO = 'sha1'
DEDUP_TOOL_MAX_THREAD = 10
OUT_DIR = '/tmp/'
COPY_PREFIX = 'copy_'

target_pool = 'default.rgw.buckets.data'
num_server = 1
pool_total_bytes = 0
ssh_clients = []
worker_threads = []

get_sigint = False
exit_signal = False
progress_read_bytes = 0
result_read_bytes = 0
result_dedup_bytes = 0
file_duplicate_bytes = 0

def human_readable_size(size):
    if size < 0:
        return '0 B'
    elif size < 1024:
        return str(size) + ' B'
    elif size < 1024 * 1024:
        return str(round(size / 1024, 2)) + ' KiB'
    elif size < 1024 * 1024 * 1024:
        return str(round(size / 1024 / 1024, 2)) + ' MiB'
    elif size < 1024 * 1024 * 1024 * 1024:
        return str(round(size / 1024 / 1024 / 1024, 2)) + ' GiB'
    else:
        return str(round(size / 1024 / 1024 / 1024 / 1024, 2)) + ' TiB'

def create_ssh_client(server, port, user, password):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client

def sigint_handler(signum, frame):
    print('SIGINT received from frame:',  frame)
    global get_sigint
    get_sigint = True

    ssh_ip = []
    for client in ssh_clients:
        if client.get_transport().getpeername()[0] not in ssh_ip:
            ssh_ip.append(client.get_transport().getpeername()[0])
            client.exec_command('ps -elf | grep ceph-dedup-tool | awk \'{print $4}\' | while read line; do kill -INT $line; done')

def worker_thread(chunk_algo, chunk_size, worker_id):
    cmd = 'ceph-dedup-tool --op estimate --pool ' + target_pool + ' --chunk-algorithm ' + chunk_algo + ' --chunk-size ' + chunk_size + ' --fingerprint-algorithm ' + FP_ALGO + ' --num-dedup-tool ' + str(num_worker) + ' --dedup-tool-id ' + str(worker_id) + ' --max-thread ' + str(DEDUP_TOOL_MAX_THREAD) + ' --report-period 1' + ' --window-bits ' + WINDOW_BITS + ' --output-dir ' + OUT_DIR
    # print("cmd: " + cmd)
    stdin, stdout, stderr = ssh_clients[worker_id].exec_command(cmd)

    json_str = ""
    start = False
    global progress_read_bytes
    global progress_dedup_bytes
    progress_read_bytes_previous = 0

    for line in stderr:
        if line.startswith('{'):
            start = True
            json_str += line
        elif line.startswith('}'):
            start = False
            json_str += line
            progress = json.loads(json_str)
            # print(progress)            
            progress_read_bytes += progress['summary']['examined_bytes'] - progress_read_bytes_previous
            progress_read_bytes_previous = progress['summary']['examined_bytes']
            json_str = ""
        else:
            if start:
                json_str += line
    
    json_str = ""
    start = False
    global result_read_bytes
    global result_dedup_bytes

    for line in stdout:
        if line.startswith('{'):
            start = True
            json_str += line
        elif line.startswith('}'):
            start = False
            json_str += line
            result = json.loads(json_str)
            # print(result)
            result_read_bytes += result['summary']['examined_bytes']
            result_dedup_bytes += result['chunk_sizes'][0]["dedup_bytes"]
            json_str = ""
        else:
            if start:
                json_str += line

    # get fp_info file into current node
    sftp = ssh_clients[worker_id].open_sftp()
    file_name = 'fp_info_' + chunk_size + '_' + str(worker_id) + '.csv'
    sftp.get(OUT_DIR + file_name, OUT_DIR + COPY_PREFIX + file_name)
    sftp.close()
    cmd = 'rm -rf ' + OUT_DIR + file_name
    ssh_clients[worker_id].exec_command(cmd)

def progress_print_thread():
    global exit_signal
    global progress_read_bytes
    progress_read_bytes_previous = 0

    while not exit_signal:
        print('\r' + ' ' * 160 + '\r', end='')
        sys.stdout.flush()
        print('\rProgress: {:>4}% / 100% (read: {:>4}/s, total: {:>7} / {:>7})'.
                  format(
                      format(progress_read_bytes / pool_total_bytes * 100, ".2f"),
                      human_readable_size(progress_read_bytes - progress_read_bytes_previous),
                      human_readable_size(progress_read_bytes),
                      human_readable_size(pool_total_bytes)),
              end='')
        sys.stdout.flush()
        progress_read_bytes_previous = progress_read_bytes
        time.sleep(1)

def find_duplicates(*file_names):
    # Open all files and read the first line from each.
    file_iters = [iter(csv.reader(open(file_name))) for file_name in file_names]
    active_lines = [next(file_iter) for file_iter in file_iters]

    # Use a heap to always get the smallest key efficiently.
    heap = [(line[0], i) for i, line in enumerate(active_lines)]
    heapq.heapify(heap)

    while heap:
        # Get the smallest key.
        min_key, min_index = heapq.heappop(heap)

        # Find all active lines with this key.
        duplicates = [active_lines[min_index]]
        duplicate_indices = [min_index]
        while heap and heap[0][0] == min_key:
            _, duplicate_index = heapq.heappop(heap)
            duplicates.append(active_lines[duplicate_index])
            duplicate_indices.append(duplicate_index)

        # If there are duplicates, yield them.
        if len(duplicates) > 1:
            yield duplicates

        # Read the next line from each file that we found a duplicate in.
        for duplicate_index in duplicate_indices:
            try:
                active_lines[duplicate_index] = next(file_iters[duplicate_index])
                heapq.heappush(heap, (active_lines[duplicate_index][0], duplicate_index))
            except StopIteration:
                pass  # This file is done.

def do_estimate(chunk_algo, chunk_size):
    global exit_signal
    global progress_read_bytes
    global result_read_bytes
    global result_dedup_bytes
    global file_duplicate_bytes

    exit_signal = False
    progress_read_bytes = 0
    result_read_bytes = 0
    result_dedup_bytes = 0
    file_duplicate_bytes = 0

    worker_threads = []
    for worker_id in range(num_worker):
        worker_threads.append(threading.Thread(target=worker_thread, args=(chunk_algo, chunk_size, worker_id)))
        worker_threads[worker_id].start()

    progress_thread = threading.Thread(target=progress_print_thread)
    progress_thread.start()

    for t in worker_threads:
        t.join()
    
    exit_signal = True
    progress_thread.join()

    chunk_files = []
    for i in range(num_worker):
        file_name = 'fp_info_' + chunk_size + '_' + str(i) + '.csv'
        chunk_files.append(OUT_DIR + COPY_PREFIX + file_name)

    for duplicates in find_duplicates(*chunk_files):
        file_duplicate_bytes += int(duplicates[0][1]) * (len(duplicates) - 1)

    for file in chunk_files:
        cmd = 'rm -rf ' + file
        subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

# main
target_pool = input('estimate target ceph pool: ')
num_worker = int(input('total numbers of worker: '))

cmd = 'ceph df --format json-pretty | jq \'.pools[] | select(.name == "' + target_pool + '") | .stats.stored\''
# print("cmd: " + cmd)
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
out, err = proc.communicate()
pool_total_bytes = int(out.decode('utf-8').replace('\n', ''))

# register signal handler
signal.signal(signal.SIGINT, sigint_handler)

for i in range(num_worker):
    server = input('server ' + str(i) + ' ip: ')
    port = input('server ' + str(i) + ' ssh port: ')
    user = input('server ' + str(i) + ' user: ')
    password = getpass.getpass('server ' + str(i) + ' password: ')

    ssh_clients.append(create_ssh_client(server, port, user, password))

for client in ssh_clients:
    stdin, stdout, stderr = client.exec_command('ceph-dedup-tool -h')
    if stdout.channel.recv_exit_status() != 0:
        print('ceph-dedup-tool is not installed on ' + client.get_transport().getpeername()[0])
        exit(1)
    print('preflight on ' + client.get_transport().getpeername()[0] + ' passed!\n\n')


f = open(RESULT_FILE, 'a')
f.write('estimate target ceph pool: ' + target_pool + '\n')
f.write('chunk_algo  chunk_size  after_dedup / pool_size (      %)  elapsed_time\n')
f.write('----------  ----------  ---------------------------------  ------------\n')
f.flush()

for chunk_algo in CHUNK_ALGORITMH_OPTIONS:
    for chunk_size in CHUNK_SIZE_OPTIONS:
        print('estimate with chunk algorithm: ' + chunk_algo + ', chunk size: ' + chunk_size)
        start_time = time.time()
        do_estimate(chunk_algo, chunk_size)
        end_time = time.time()
        print('\r' + ' ' * 160 + '\r', end='')
        print('elapsed time: {}, Result: {:>7} / {:>7} ({:>6}%)\n'.format(
                  str(round(end_time - start_time)) + 's',  
                  human_readable_size(result_dedup_bytes - file_duplicate_bytes),
                  human_readable_size(pool_total_bytes),
                  format((result_dedup_bytes - file_duplicate_bytes) / pool_total_bytes * 100, ".2f")))
        
        f.write('{:>10}  {:>10}  {:>11} / {:>9} ({:>6}%)  {:>12}\n'.format(
            chunk_algo, chunk_size, 
            human_readable_size((result_dedup_bytes - file_duplicate_bytes)), human_readable_size(pool_total_bytes),
            format((result_dedup_bytes - file_duplicate_bytes) / pool_total_bytes * 100, ".2f"),
            str(round(end_time - start_time)) + 's'))
        f.flush()

        if get_sigint:
            break
    if get_sigint:
        break

f.write('\n')
f.flush()
f.close()
