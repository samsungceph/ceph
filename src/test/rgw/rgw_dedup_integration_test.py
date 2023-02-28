
import subprocess
import os
import time
import threading
import json

path = os.getcwd()
os.chdir('../../../build/')
subprocess.run('rm -rf ./rgw_test_tmp_dir', shell=True)
subprocess.run('mkdir ./rgw_test_tmp_dir', shell=True)

access_key = '0555b35654ad1656d804'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

bucket_name = 'test-bucket'


def create_file(file_type):
    print('create temporary test directory (rgw_test_tmp_dir)')

    cmd = 'dd if=/dev/'
    if file_type == 'zero':
        print('create 100MiB of zero-filled file')
        cmd += 'zero of=rgw_test_tmp_dir/zero_file '
    elif file_type == 'rand':
        print('create 100MiB of random file')
        cmd += 'urandom of=rgw_test_tmp_dir/rand_file '

    cmd += 'bs=100M count=1'
    print(cmd)
    subprocess.run(cmd, shell=True)


def delete_files():
    print('delete temporary test directory')
    subprocess.run('rm -rf ./rgw_test_tmp_dir', shell=True)


def put_file(file_type):
    if file_type == 'zero':
        subprocess.run('s3cmd put rgw_test_tmp_dir/zero_file s3://' + bucket_name
            + ' --access_key=' + access_key + ' --secret_key=' + secret_key
            + ' --host=127.0.0.1:8000', shell=True)
    elif file_type == 'rand':
        subprocess.run('s3cmd put rgw_test_tmp_dir/rand_file s3://' + bucket_name
            + ' --access_key=' + access_key + ' --secret_key=' + secret_key
            + ' --host=127.0.0.1:8000', shell=True)


def run_vstart():
    print('starts vstart cluster')
    subprocess.run('rm -rf out/*', shell=True)
    subprocess.run('MON=1 OSD=3 MGR=1 RGW=1 NFS=0 MDS=0 ../src/vstart.sh -d -n --without-dashboard', shell=True)


def stop_vstart():
    print('stops vstart cluster')
    subprocess.run('../src/stop.sh', shell=True)


def read_file(file_type, event):
    while True:
        if event.is_set():
            print('Stop reading ' + file_type + '_file')
            break

        print('read ' + file_type + '_file')
        if file_type == 'zero':
            subprocess.run('s3cmd get -f s3://' + bucket_name + '/zero_file ./rgw_test_tmp_dir/get_zero_file'
                + ' --access_key=' + access_key + ' --secret_key=' + secret_key + ' --host=127.0.0.1:8000',
                shell=True)
        elif file_type == 'rand':
            subprocess.run('s3cmd get -f s3://' + bucket_name + '/rand_file ./rgw_test_tmp_dir/get_rand_file'
                + ' --access_key=' + access_key + ' --secret_key=' + secret_key + ' --host=127.0.0.1:8000',
                shell=True)

        time.sleep(10)


# pass condition: cold pool usage (= 64KiB) and chunk ref count (>= 1600)
def check_dedup_test_done(event):
    print('Check if dedup test is done')
    while not event.is_set():
        ret = subprocess.run('bin/ceph df', shell=True, text=True, capture_output=True).stdout
        if len(ret) <= 0:
            time.sleep(1)
            continue

        for line in ret.split('\n'):
            if len(line.split()) <= 0:
                continue

            fields = line.split()
            if fields[0].strip() == 'default.rgw.buckets.data_cold':
                if int(fields[3].strip()) == 64 and fields[4].strip() == 'KiB':
                    chunk_oid = ''
                    while len(chunk_oid) <= 0:
                        chunk_oid = subprocess.run('bin/rados ls -p default.rgw.buckets.data_cold | head -1',
                            shell=True, text=True, capture_output=True).stdout.strip()
                        time.sleep(1)
                    ref_count = int(json.loads(get_chunk_ref_dump(chunk_oid))['count'])
                    print(chunk_oid + ' ref count: ' + str(ref_count))
                    if ref_count >= 1600:
                        print('Dedup Test PASSED')
                        return
                    else:
                        break
                else:
                    break

        time.sleep(1)

# pass condition: base pool object list and cold pool object list is same
def check_migration_test_done(event):
    print('Check if migration test is done')

    is_done = False
    while (not event.is_set()) and (not is_done):
        ret = subprocess.run('bin/rados ls -p default.rgw.buckets.data | grep rand_file',
                shell=True, text=True, capture_output=True).stdout
        if len(ret) <= 0:
            time.sleep(1)
            continue

        base_pool_list = ret.strip().split('\n')

        # remove head object (= has no data)
        for obj in base_pool_list:
            if obj.find('shadow') > 0 or obj.find('multipart') > 0:
                continue
            base_pool_list.remove(obj)

        ret = subprocess.run('bin/rados ls -p default.rgw.buckets.data_cold',
                shell=True, text=True, capture_output=True).stdout
        if len(ret) <= 0:
            time.sleep(1)
            continue

        chunk_pool_list = ret.strip().split('\n')

        tmplist = base_pool_list[:]
        for obj in chunk_pool_list:
            tmplist.remove(obj)

        if len(tmplist) == 0:
            print('rand_file\'s rados object list: ', base_pool_list)
            print('flushed rand_file\'s object list: ', chunk_pool_list)
            print('Migration Test PASSED')
            is_done = True

        time.sleep(1)



# pass condition: dummy_obj is removed in reference list of chunk_oid
def check_scrub_test_done(event, chunk_oid):
    print('Check if scrub test is done')
    refs = json.loads(get_chunk_ref_dump(chunk_oid))['refs']

    is_ref_mismatch = False
    while not event.is_set():
        refs = json.loads(get_chunk_ref_dump(chunk_oid))['refs']
        is_ref_mismatch = False
        for ref in refs:
            if ref['oid'].strip() == 'dummy_obj':
                is_ref_mismatch = True
                time.sleep(1)

        if not is_ref_mismatch:
            print(chunk_oid + '\'s mismatch repaired')
            print(get_chunk_ref_dump(chunk_oid))
            print('Scrub Test PASSED')
            return


def test_state_check(test_title, thread):
    print('Waiting for ' + test_title + ' is done')

    retry_cnt = 0
    test_result = False
    # check test result every 10 secs during 5 mins
    while retry_cnt < 30:
        if not thread.is_alive():
            print('TEST FINISHED')
            test_result = True
            break
        time.sleep(10)
        retry_cnt += 1

    if not test_result:
        # test fail
        print(test_title + ' FAILED')
        thread.set()
        stop_vstart()
        delete_files()



def get_chunk_ref_dump(chunk_oid):
    return subprocess.run('bin/ceph-dedup-tool --op dump-chunk-refs --chunk-pool '
        + 'default.rgw.buckets.data_cold --object ' + chunk_oid,
        shell=True, text=True, capture_output=True).stdout


def test_scrub(event):
    print('RGWDedup Scrub Test Starts')
    create_file('rand')
    run_vstart()

    # wait for rgw is ready
    time.sleep(5)

    subprocess.run('s3cmd mb s3://' + bucket_name + ' --access_key=' + access_key
        + ' --secret_key=' + secret_key + ' --host=127.0.0.1:8000', shell=True)
    put_file('rand')

    # wait for an object is deduped
    print('Wait for dedup is done')
    time.sleep(10)

    chunk_oid = ''
    while len(chunk_oid) <= 0:
        chunk_oid = subprocess.run('bin/rados ls -p default.rgw.buckets.data_cold | head -1',
            shell=True, text=True, capture_output=True).stdout.strip()
        time.sleep(1)
    print(chunk_oid + '\'s normal check refs dump')
    print(get_chunk_ref_dump(chunk_oid))

    # make chunk object mismatch
    subprocess.run('bin/ceph-dedup-tool --op chunk-get-ref --chunk-pool default.rgw.buckets.data_cold '
        + '--object ' + chunk_oid + ' --target-ref dummy_obj --target-ref-pool-id 999',
        shell=True, text=True, capture_output=True)
    print(chunk_oid + '\'s mismatched chunk refs dump')
    print(get_chunk_ref_dump(chunk_oid))

    # waiting for RGWScrubWorker fixes ref mismatch
    check_scrub_test_done(event, chunk_oid)

    # test_scrub passed
    event.set()
    stop_vstart()
    delete_files()



if __name__ == '__main__':
    print('RGWDedup System Integration Tests Start')

    #  Scrub test
    event = threading.Event()
    scrub_thread = threading.Thread(target=test_scrub, args=(event,))
    scrub_thread.start()
    time.sleep(5)
    test_state_check('Scrub Test', scrub_thread)

    print('RGWDedup System Integration Tests Done')

