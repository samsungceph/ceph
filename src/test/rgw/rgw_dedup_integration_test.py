import os
import random
import sys
import time
import json
import threading
from subprocess import Popen, PIPE
from threading import Thread


VERBOSE = False
BASE_POOL = "default.rgw.buckets.data"
COLD_POOL = "default-cold-pool"
DOWNLOAD_POSTFIX = "_download"
DEFAULT_CHUNK_SIZE = 16 * 1024 * 4 * 2
MULTIPART_CHUNK_SIZE = 15 * 1024 * 1024
CEPH_RADOS_OBJECT_SIZE = 4 * 1024 * 1024
MAX_CHUNK_REF_SIZE = 10000
RGW_INSTANCE_NUM = 3
DEDUP_WORKER_THREAD_NUM = 3
THREAD_EXIT_SIGNAL = False
THREAD_SUCCESS = True
TEST_SUCCESS = True

IMAGE_NAME = "10.10.40.35:5000/testimage"

BOOTSTRAP_NODE_IP = "10.10.40.35"
BOOTSTRAP_NODE_PW = ""

OSD_NODE_1_IP = "10.10.40.40"
OSD_NODE_1_PW = ""
OSD_NODE_1_DISK_1 = "/dev/nvme21n1"
OSD_NODE_1_DISK_2 = "/dev/nvme22n1"
OSD_NODE_1_DISK_3 = "/dev/nvme23n1"

OSD_NODE_2_IP = "10.10.40.41"
OSD_NODE_2_PW = ""
OSD_NODE_2_DISK_1 = "/dev/nvme21n1"
OSD_NODE_2_DISK_2 = "/dev/nvme22n1"
OSD_NODE_2_DISK_3 = "/dev/nvme23n1"

OSD_NODE_3_IP = "10.10.40.42"
OSD_NODE_3_PW = ""
OSD_NODE_3_DISK_1 = "/dev/nvme21n1"
OSD_NODE_3_DISK_2 = "/dev/nvme22n1"
OSD_NODE_3_DISK_3 = "/dev/nvme23n1"

RGW_NODE_IP = "10.10.40.39"
RGW_NODE_PW = ""
RGW_VIRTUAL_IP = "10.10.40.100"

OSD_NODE_1_HOSTNAME = "testosdnode1"
OSD_NODE_2_HOSTNAME = "testosdnode2"
OSD_NODE_3_HOSTNAME = "testosdnode3"
RGW_NODE_HOSTNAME = "testrgwnode"


def log(msg):
    global VERBOSE
    if VERBOSE:
        print(msg)

def bootstrap_ceph():
    print("Bootstrap Ceph cluster START")

    # bootstrap
    cmd = "sudo ./cephadm --image {} bootstrap --mon-ip {} --allow-mismatched-release --fsid 12341234-1234-1234-1234-123412341234 --initial-dashboard-password 12341234 --dashboard-password-noupdate".format(IMAGE_NAME, BOOTSTRAP_NODE_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo ceph -s"
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    # discover hostname & add hosts
    cmd = "sudo sshpass -p{} ssh-copy-id -f -i /etc/ceph/ceph.pub root@{}".format(OSD_NODE_1_PW, OSD_NODE_1_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo sshpass -p{} ssh-copy-id -f -i /etc/ceph/ceph.pub root@{}".format(OSD_NODE_2_PW, OSD_NODE_2_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo sshpass -p{} ssh-copy-id -f -i /etc/ceph/ceph.pub root@{}".format(OSD_NODE_3_PW, OSD_NODE_3_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo sshpass -p{} ssh-copy-id -f -i /etc/ceph/ceph.pub root@{}".format(RGW_NODE_PW, RGW_NODE_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    global OSD_NODE_1_HOSTNAME
    cmd = "sudo cat /etc/hosts | grep {} | awk '{{print $2}}'".format(OSD_NODE_1_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    OSD_NODE_1_HOSTNAME = p.communicate()[0].decode().strip()

    cmd = "sudo ceph orch host add {} {}".format(OSD_NODE_1_HOSTNAME, OSD_NODE_1_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    global OSD_NODE_2_HOSTNAME
    cmd = "sudo cat /etc/hosts | grep {} | awk '{{print $2}}'".format(OSD_NODE_2_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    OSD_NODE_2_HOSTNAME = p.communicate()[0].decode().strip()

    cmd = "sudo ceph orch host add {} {}".format(OSD_NODE_2_HOSTNAME, OSD_NODE_2_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    global OSD_NODE_3_HOSTNAME
    cmd = "sudo cat /etc/hosts | grep {} | awk '{{print $2}}'".format(OSD_NODE_3_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    OSD_NODE_3_HOSTNAME = p.communicate()[0].decode().strip()

    cmd = "sudo ceph orch host add {} {}".format(OSD_NODE_3_HOSTNAME, OSD_NODE_3_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    global RGW_NODE_HOSTNAME
    cmd = "sudo cat /etc/hosts | grep {} | awk '{{print $2}}'".format(RGW_NODE_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    RGW_NODE_HOSTNAME = p.communicate()[0].decode().strip()

    cmd = "sudo ceph orch host add {} {}".format(RGW_NODE_HOSTNAME, RGW_NODE_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    time.sleep(30)

    # add OSDs
    cmd = "sudo ceph orch daemon add osd {}:{}".format(OSD_NODE_1_HOSTNAME, OSD_NODE_1_DISK_1)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo ceph orch daemon add osd {}:{}".format(OSD_NODE_1_HOSTNAME, OSD_NODE_1_DISK_2)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo ceph orch daemon add osd {}:{}".format(OSD_NODE_1_HOSTNAME, OSD_NODE_1_DISK_3)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo ceph orch daemon add osd {}:{}".format(OSD_NODE_2_HOSTNAME, OSD_NODE_2_DISK_1)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo ceph orch daemon add osd {}:{}".format(OSD_NODE_2_HOSTNAME, OSD_NODE_2_DISK_2)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo ceph orch daemon add osd {}:{}".format(OSD_NODE_2_HOSTNAME, OSD_NODE_2_DISK_3)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo ceph orch daemon add osd {}:{}".format(OSD_NODE_3_HOSTNAME, OSD_NODE_3_DISK_1)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo ceph orch daemon add osd {}:{}".format(OSD_NODE_3_HOSTNAME, OSD_NODE_3_DISK_2)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo ceph orch daemon add osd {}:{}".format(OSD_NODE_3_HOSTNAME, OSD_NODE_3_DISK_3)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    # add RGWs
    cmd = "sudo ceph orch apply rgw testrgw \"--placement=3 {}\" --port=8080".format(RGW_NODE_HOSTNAME)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    time.sleep(30)

    # add LB
    cmd = "sudo cat << EOF > ingress.yaml\nservice_type: ingress\nservice_id: rgw.testrgw\nservice_name: ingress.rgw.testrgw\nplacement:\n  hosts:\n  - {}\nspec:\n  backend_service: rgw.testrgw\n  frontend_port: 80\n  monitor_port: 1967\n  virtual_ip: {}\nEOF".format(RGW_NODE_HOSTNAME, RGW_VIRTUAL_IP)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo ceph orch apply -i ingress.yaml"
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo rm ingress.yaml"
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    # add s3 user
    cmd = "sudo radosgw-admin user create --uid=\"1234\" --display-name=\"1234\" --access-key=\"0555b35654ad1656d804\" --secret-key=\"h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==\""
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    # set dedup config
    time.sleep(10)
    cmd = "sudo ceph config set global rgw_enable_dedup_threads true"
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    
    time.sleep(10)
    cmd = "sudo ceph orch restart rgw.testrgw"
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    time.sleep(60)
    print("Bootstrap Ceph cluster END")

def destroy_ceph():
    # cleanup
    cmd = "sudo sshpass -p{} ssh root@{} ./cephadm --image {} rm-cluster --force --fsid 12341234-1234-1234-1234-123412341234 --zap-osds".format(BOOTSTRAP_NODE_PW, BOOTSTRAP_NODE_IP, IMAGE_NAME)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo sshpass -p{} ssh root@{} ./cephadm --image {} rm-cluster --force --fsid 12341234-1234-1234-1234-123412341234 --zap-osds".format(OSD_NODE_1_PW, OSD_NODE_1_IP, IMAGE_NAME)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo sshpass -p{} ssh root@{} ./cephadm --image {} rm-cluster --force --fsid 12341234-1234-1234-1234-123412341234 --zap-osds".format(OSD_NODE_2_PW, OSD_NODE_2_IP, IMAGE_NAME)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo sshpass -p{} ssh root@{} ./cephadm --image {} rm-cluster --force --fsid 12341234-1234-1234-1234-123412341234 --zap-osds".format(OSD_NODE_3_PW, OSD_NODE_3_IP, IMAGE_NAME)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    cmd = "sudo sshpass -p{} ssh root@{} ./cephadm --image {} rm-cluster --force --fsid 12341234-1234-1234-1234-123412341234 --zap-osds".format(RGW_NODE_PW, RGW_NODE_IP, IMAGE_NAME)
    log("bootstrap_ceph() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()


def create_file(mode, file_name, file_size):
    if mode == "zero":
        cmd = "dd if=/dev/zero of=/tmp/{} bs={} count=1".format(file_name, file_size)
    elif mode == "random":
        cmd = "dd if=/dev/urandom of=/tmp/{} bs={} count=1".format(file_name, file_size)
    elif mode == "random-with-hole":
        create_file("random", file_name, file_size)
        cmd = "dd if=/dev/zero of=/tmp/{} bs=1 count={} seek={} conv=notrunc".format(file_name, DEFAULT_CHUNK_SIZE, 0)
        cmd += "; dd if=/dev/zero of=/tmp/{} bs=1 count={} seek={} conv=notrunc".format(file_name, DEFAULT_CHUNK_SIZE, get_file_size(file_name) // 2)
        cmd += "; dd if=/dev/zero of=/tmp/{} bs=1 count={} seek={} conv=notrunc".format(file_name, DEFAULT_CHUNK_SIZE, get_file_size(file_name) - DEFAULT_CHUNK_SIZE)
    elif mode == "big-random":
        cmd = "dd if=/dev/urandom of=/tmp/{} bs={} count=5".format(file_name, file_size)
    else:
        raise Exception("mode is not valid")

    log("create_file() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("create_file() failed: {}".format(err))

def get_file_size(file_name):
    cmd = "stat -c %s /tmp/{}".format(file_name)
    log("get_file_size() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("get_file_size() failed: {}".format(err))
    log("get_file_size() out: {}".format(out))
    return int(out)

# return file's rados object(4MB) count
def get_file_rados_object_count(file_size):
    log("get_file_rados_object_count() file_size: {}".format(file_size))
    base = file_size // MULTIPART_CHUNK_SIZE
    remain = file_size % MULTIPART_CHUNK_SIZE
    if remain == 0:
        log("get_file_rados_object_count() return: {}".format(base * 4))
        return int(base * 4)
    elif 0 < remain <= CEPH_RADOS_OBJECT_SIZE:
        log("get_file_rados_object_count() return: {}".format(base * 4 + 1))
        return int(base * 4 + 1)
    elif CEPH_RADOS_OBJECT_SIZE < remain <= CEPH_RADOS_OBJECT_SIZE * 2:
        log("get_file_rados_object_count() return: {}".format(base * 4 + 2))
        return int(base * 4 + 2)
    elif CEPH_RADOS_OBJECT_SIZE * 2 < remain <= CEPH_RADOS_OBJECT_SIZE * 3:
        log("get_file_rados_object_count() return: {}".format(base * 4 + 3))
        return int(base * 4 + 3)

def get_file_checksum(file_name):
    cmd = "md5sum /tmp/{}".format(file_name)
    log("get_checksum() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("get_checksum() failed: {}".format(err))
    log("get_file_checksum() out: {}".format(out.decode().split()[0]))
    return out.decode().split()[0]

def delete_file(file_name):
    cmd = "rm -rf /tmp/{}".format(file_name)
    log("delete_file() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("delete_file() failed: {}".format(err))


def create_rgw_bucket(bucket_name):
    cmd = "s3cmd mb --host={} s3://{}".format(RGW_VIRTUAL_IP, bucket_name)
    log("create_rgw_bucket() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("create_rgw_bucket() failed: {}".format(err))

def upload_rgw_object(bucket_name, file_name):
    cmd = "s3cmd put --host={} /tmp/{} s3://{}/".format(RGW_VIRTUAL_IP, file_name, bucket_name)
    log("upload_rgw_object() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("upload_rgw_object() failed: {}".format(err))

def download_rgw_object(bucket_name, file_name):
    cmd = "s3cmd get --host={} s3://{}/{} /tmp/{} --force".format(RGW_VIRTUAL_IP, bucket_name, file_name, file_name + DOWNLOAD_POSTFIX)
    log("download_rgw_object() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("download_rgw_object() failed: {}".format(err))
    
def delete_rgw_object(bucket_name, file_name):
    cmd = "s3cmd rm --host={} s3://{}/{}".format(RGW_VIRTUAL_IP, bucket_name, file_name)
    log("delete_rgw_object() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("delete_rgw_object() failed: {}".format(err))
    
def delete_rgw_bucket(bucket_name):
    cmd = "s3cmd rb --host={} s3://{} --recursive".format(RGW_VIRTUAL_IP, bucket_name)
    log("delete_rgw_bucket() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("delete_rgw_bucket() failed: {}".format(err))


def upload_rados_object(pool_name, file_name):
    cmd = "rados -p {} put {} /tmp/{}".format(pool_name, file_name, file_name)
    log("upload_rados_object() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("upload_rados_object() failed: {}".format(err))

def download_rados_object(pool_name, object_name):
    cmd = "rados -p {} get {} /tmp/{}".format(pool_name, object_name, object_name + DOWNLOAD_POSTFIX)
    log("download_rados_object() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("download_rados_object() failed: {}".format(err))

def list_rados_pool(pool_name):
    cmd = "rados -p {} ls".format(pool_name)
    log("list_rados_pool() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("list_rados_pool() failed: {}".format(err))
    return out.decode().split()

def stat_rados_object(pool_name, object_name):
    cmd = "rados -p {} stat {}".format(pool_name, object_name)
    log("stat_rados_object() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    return out.decode()
      
def delete_rados_object(pool_name, object_name):
    cmd = "rados -p {} rm {}".format(pool_name, object_name)
    log("delete_rados_object() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("delete_rados_object() failed: {}".format(err))


def clear_rgw():
    cmd = "radosgw-admin gc process --include-all"
    log("clear_rgw() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("clear_rgw() failed: {}".format(err))

def clear_cold_pool():
    cmd = "rados purge {} --yes-i-really-really-mean-it".format(COLD_POOL)
    log("clear_cold_pool() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("clear_cold_pool() failed: {}".format(err))


def create_and_upload_file_thread(thread_id, bucket_name, num_files, mode, file_size):
    log("create_and_upload_file_thread {} start".format(thread_id))
    for i in range(num_files):
        file_name = "testthread{}_testfile{}".format(thread_id, i)
        create_file(mode, file_name, file_size)
        upload_rgw_object(bucket_name, file_name)
    log("create_and_upload_file_thread {} end".format(thread_id))

def download_and_compare_file_thread(thread_id, bucket_name, num_files):
    global THREAD_SUCCESS
    log("download_and_compare_file_thread {} start".format(thread_id))
    for i in range(num_files):
        file_name = "testthread{}_testfile{}".format(thread_id, i)
        download_rgw_object(bucket_name, file_name)
        if get_file_checksum(file_name) != get_file_checksum(file_name + DOWNLOAD_POSTFIX):
            THREAD_SUCCESS = False
    log("download_and_compare_file_thread {} end".format(thread_id))

def delete_file_thread(thread_id, bucket_name, num_files):
    log("delete_file_thread {} start".format(thread_id))
    for i in range(num_files):
        file_name = "testthread{}_testfile{}".format(thread_id, i)
        delete_rgw_object(bucket_name, file_name)
        delete_file(file_name)
        delete_file(file_name + DOWNLOAD_POSTFIX)
    log("delete_file_thread {} end".format(thread_id))


# threads for workload generation
def read_load_prepare_thread(thread_id, bucket_name, num_files):
    log("read_load_prepare_thread {} start".format(thread_id))
    for i in range(num_files):
        file_name = "testthread{}_testreadfile{}".format(thread_id, i)
        create_file("random", file_name, "10M")
        upload_rgw_object(bucket_name, file_name)
    log("read_load_prepare_thread {} end".format(thread_id))

def write_load_prepare_thread(thread_id, num_files):
    log("write_load_prepare_thread {} start".format(thread_id))
    for i in range(num_files):
        file_name = "testthread{}_testwritefile{}".format(thread_id, i)
        create_file("random", file_name, "10M")
    log("write_load_prepare_thread {} end".format(thread_id))

def read_load_thread(thread_id, bucket_name, num_files):
    log("read_load_thread {} start".format(thread_id))
    
    while not THREAD_EXIT_SIGNAL:
        file_name = "testthread{}_testreadfile{}".format(thread_id, random.randint(0, num_files - 1))
        download_rgw_object(bucket_name, file_name)
    
    log("read_load_thread {} end".format(thread_id))

def write_load_thread(thread_id, bucket_name, num_files):
    log("write_load_thread {} start".format(thread_id))
    
    while not THREAD_EXIT_SIGNAL:
        file_name = "testthread{}_testwritefile{}".format(thread_id, random.randint(0, num_files - 1))
        upload_rgw_object(bucket_name, file_name)
        
    log("write_load_thread {} end".format(thread_id))

def delete_read_load_thread(thread_id, bucket_name, num_read_files):
    log("delete_read_load_thread {} start".format(thread_id))
    for i in range(num_read_files):
        read_file_name = "testthread{}_testreadfile{}".format(thread_id, i)
        delete_rgw_object(bucket_name, read_file_name)
        delete_file(read_file_name)
        delete_file(read_file_name + DOWNLOAD_POSTFIX)
    log("delete_read_load_thread {} end".format(thread_id))

def delete_write_load_thread(thread_id, bucket_name, num_write_files):
    log("delete_write_load_thread {} start".format(thread_id))
    for i in range(num_write_files):
        write_file_name = "testthread{}_testwritefile{}".format(thread_id, i)
        delete_rgw_object(bucket_name, write_file_name)
        delete_file(write_file_name)
    log("delete_write_load_thread {} end".format(thread_id))


def preflight_check():
    print("Preflight check start")
    print("1. checking ceph... ", end="")
    cmd = "ceph -s"
    log("preflight_check() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        print("failed!")
        raise Exception("ceph -s failed: {}".format(err))
    print("OK!")
    
    print("2. checking pools exists... ", end="")
    cmd = "rados lspools"
    log("preflight_check() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        print("failed!")
        raise Exception("rados lspools failed: {}".format(err))
    if BASE_POOL not in out.decode():
        print("failed!")
        raise Exception("base pool not found: {}".format(BASE_POOL))
    if COLD_POOL not in out.decode():
        print("failed!")
        raise Exception("cold pool not found: {}".format(COLD_POOL))
    print("OK!")

    print("3. checking s3cmd... ", end="")
    cmd = "s3cmd --host=" + RGW_VIRTUAL_IP + " ls"
    log("preflight_check() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        print("failed!")
        raise Exception("s3cmd ls failed: {}".format(err))
    print("OK!")

    print("4. checking ceph-dedup-tool... ", end="")
    cmd = "ceph-dedup-tool --help"
    log("preflight_check() cmd: {}".format(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        print("failed!")
        raise Exception("ceph-dedup-tool --help failed: {}".format(err))
    print("OK!")
    print("Preflight check end\n")


# 01. archive small file with no data duplication
def test01():
    global TEST_SUCCESS

    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    file_mode = "random"
    file_name = "testfile"
    file_size = "4M"
    create_file(file_mode, file_name, file_size)
    
    upload_rgw_object(bucket_name, file_name)

    while True:
        if len(list_rados_pool(COLD_POOL)) == 0:
            log("waiting for cold object creation...")
            time.sleep(1)
        elif len(list_rados_pool(COLD_POOL)) == 1:
            archieve_object_name = list_rados_pool(COLD_POOL)[0]
            log("cold object created, cold object name: {}".format(archieve_object_name))
            break
        else:
            raise Exception("cold object more than 1, expected: 1, actual: {}".format(len(list_rados_pool(COLD_POOL))))

    stat_rados_object(COLD_POOL, archieve_object_name)

    download_rados_object(COLD_POOL, archieve_object_name)

    log("checksum of uploaded file: {}".format(get_file_checksum(file_name)))
    log("checksum of downloaded file: {}".format(get_file_checksum(archieve_object_name + DOWNLOAD_POSTFIX)))
    if get_file_checksum(file_name) == get_file_checksum(archieve_object_name + DOWNLOAD_POSTFIX):
        print("TEST01 passed!")
    else:
        print("TEST01 failed!")
        TEST_SUCCESS = False

    delete_rgw_object(bucket_name, file_name)
    delete_file(file_name)
    delete_file(archieve_object_name + DOWNLOAD_POSTFIX)
    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 02. dedup small file with many data duplication
def test02():
    global TEST_SUCCESS

    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    file_mode = "zero"
    file_name = "testfile"
    file_size = "1M"
    create_file(file_mode, file_name, file_size)

    upload_rgw_object(bucket_name, file_name)

    while True:
        if len(list_rados_pool(COLD_POOL)) == 0:
            log("waiting for cold object creation...")
            time.sleep(1)
        elif len(list_rados_pool(COLD_POOL)) == 1:
            cold_object_name = list_rados_pool(COLD_POOL)[0]
            log("cold object created, cold object name: {}".format(cold_object_name))
            break
        else:
            raise Exception("cold object more than 1, expected: 1, actual: {}".format(len(list_rados_pool(COLD_POOL))))
        
    stat_rados_object(COLD_POOL, cold_object_name)

    download_rados_object(COLD_POOL, cold_object_name)

    if (len(list_rados_pool(BASE_POOL)) == 1):
        base_object_name = list_rados_pool(BASE_POOL)[0]
        log("base object name: {}".format(base_object_name))
        stat_rados_object(BASE_POOL, base_object_name)
        download_rados_object(BASE_POOL, base_object_name)
    else:
        raise Exception("base object mismatch, expected: 1, actual: {}".format(len(list_rados_pool(BASE_POOL))))

    log("checksum of uploaded file: {}".format(get_file_checksum(file_name)))
    log("checksum of downloaded file: {}".format(get_file_checksum(cold_object_name + DOWNLOAD_POSTFIX)))
    log("size of downloaded cold object: {}".format(get_file_size(cold_object_name + DOWNLOAD_POSTFIX)))
    if get_file_checksum(file_name) == get_file_checksum(base_object_name + DOWNLOAD_POSTFIX) and get_file_size(cold_object_name + DOWNLOAD_POSTFIX) == 65536:
        print("TEST02 passed!")
    else:
        print("TEST02 failed!")
        TEST_SUCCESS = False

    delete_rgw_object(bucket_name, file_name)
    delete_file(file_name)
    delete_file(cold_object_name + DOWNLOAD_POSTFIX)
    delete_file(base_object_name + DOWNLOAD_POSTFIX)
    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 03. dedup small file with partial data duplication
def test03():
    global TEST_SUCCESS
    
    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    file_mode = "random-with-hole"
    file_name = "testfile"
    file_size = "1M"
    create_file(file_mode, file_name, file_size)

    upload_rgw_object(bucket_name, file_name)

    while True:
        if len(list_rados_pool(COLD_POOL)) == 0:
            log("waiting for cold object creation...")
            time.sleep(1)
        elif len(list_rados_pool(COLD_POOL)) == 1:
            chunk_object_name = list_rados_pool(COLD_POOL)[0]
            log("cold object created, cold object name: {}".format(chunk_object_name))
            break
        else:
            raise Exception("cold object more than 1, expected: 1, actual: {}".format(len(list_rados_pool(COLD_POOL))))
    
    stat_rados_object(COLD_POOL, chunk_object_name)

    download_rados_object(COLD_POOL, chunk_object_name)

    if (len(list_rados_pool(BASE_POOL)) == 1):
        base_object_name = list_rados_pool(BASE_POOL)[0]
        log("base object name: {}".format(base_object_name))
        stat_rados_object(BASE_POOL, base_object_name)
        download_rados_object(BASE_POOL, base_object_name)
    else:
        raise Exception("base object mismatch, expected: 1, actual: {}".format(len(list_rados_pool(BASE_POOL))))
    
    log("checksum of uploaded file: {}".format(get_file_checksum(file_name)))
    log("checksum of downloaded file: {}".format(get_file_checksum(base_object_name + DOWNLOAD_POSTFIX)))
    log("size of downloaded cold object: {}".format(get_file_size(chunk_object_name + DOWNLOAD_POSTFIX)))
    if get_file_checksum(file_name) == get_file_checksum(base_object_name + DOWNLOAD_POSTFIX) and get_file_size(chunk_object_name + DOWNLOAD_POSTFIX) == 65536:
        print("TEST03 passed!")
    else:
        print("TEST03 failed!")
        TEST_SUCCESS = False

    delete_rgw_object(bucket_name, file_name)
    delete_file(file_name)
    delete_file(base_object_name + DOWNLOAD_POSTFIX)
    delete_file(chunk_object_name + DOWNLOAD_POSTFIX)
    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 04. archive many small files with no data duplication
def test04():
    global TEST_SUCCESS
    
    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    file_count = 1000
    file_mode = "random"
    file_name = "testfile"
    file_size = "4M"    
    for i in range(file_count):
        create_file(file_mode, file_name + str(i), file_size)

    for i in range(file_count):
        upload_rgw_object(bucket_name, file_name + str(i))

    while True:
        if len(list_rados_pool(COLD_POOL)) < file_count:
            log("waiting for cold object creation...")
            time.sleep(1)
        elif len(list_rados_pool(COLD_POOL)) == file_count:
            log("cold object created, cold object count: {}".format(len(list_rados_pool(COLD_POOL))))
            break
        else:
            raise Exception("cold object more than {}, expected: {}, actual: {}".format(file_count, file_count, len(list_rados_pool(COLD_POOL))))

    failed = False
    for i in range(file_count):
        download_rgw_object(bucket_name, file_name + str(i))
        if get_file_checksum(file_name + str(i)) != get_file_checksum(file_name + str(i) + DOWNLOAD_POSTFIX):
            failed = True
    
    if not failed:
        print("TEST04 passed!")
    else:
        print("TEST04 failed!")
        TEST_SUCCESS = False

    for i in range(file_count):
        delete_rgw_object(bucket_name, file_name + str(i))
        delete_file(file_name + str(i))
        delete_file(file_name + str(i) + DOWNLOAD_POSTFIX)
    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 05. archive big file with no data duplication
def test05():
    global TEST_SUCCESS

    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    file_mode = "random"
    file_name = "testfile"
    file_size = "1G"
    
    create_file(file_mode, file_name, file_size)

    upload_rgw_object(bucket_name, file_name)
    
    while True:
        if len(list_rados_pool(COLD_POOL)) < get_file_rados_object_count(get_file_size(file_name)):
            log("waiting for base object creation...")
            time.sleep(1)
        elif len(list_rados_pool(COLD_POOL)) == get_file_rados_object_count(get_file_size(file_name)):
            log("archive object created, archive object count: {}".format(len(list_rados_pool(COLD_POOL))))
            break
        else:
            raise Exception("cold object more than {}, expected: {}, actual: {}".format(get_file_rados_object_count(get_file_size(file_name)), get_file_rados_object_count(get_file_size(file_name)), len(list_rados_pool(COLD_POOL))))

    download_rgw_object(bucket_name, file_name)

    log("checksum of uploaded file: {}".format(get_file_checksum(file_name)))
    log("checksum of downloaded file: {}".format(get_file_checksum(file_name + DOWNLOAD_POSTFIX)))
    if get_file_checksum(file_name) == get_file_checksum(file_name + DOWNLOAD_POSTFIX):
        print("TEST05 passed!")
    else:
        print("TEST05 failed!")
        TEST_SUCCESS = False
    
    delete_rgw_object(bucket_name, file_name)
    delete_file(file_name)
    delete_file(file_name + DOWNLOAD_POSTFIX)
    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 06. archive big file with many data duplication without chunk ref flooding
def test06():
    global TEST_SUCCESS

    bucket_name = "testbucket"
    file_mode = "zero"
    file_name = "testfile"
    file_size = "2G"

    create_rgw_bucket(bucket_name)

    create_file(file_mode, file_name, file_size)

    upload_rgw_object(bucket_name, file_name)

    while True:
        if len(list_rados_pool(COLD_POOL)) < 1:
            log("waiting for base object creation...")
            time.sleep(1)
        elif len(list_rados_pool(COLD_POOL)) == 1:
            cold_object_name = list_rados_pool(COLD_POOL)[0]
            log("cold object created, cold object count: {}".format(len(list_rados_pool(COLD_POOL))))
            break
        else:
            raise Exception("cold object more than 1, expected: 1, actual: {}".format(len(list_rados_pool(COLD_POOL))))
    
    log("sleep 300 seconds for more dedup...")
    time.sleep(300)
    
    json_data = os.popen("ceph-dedup-tool --op dump-chunk-refs --chunk-pool default-cold-pool --object {}".format(cold_object_name)).read()
    log("cmd: ceph-dedup-tool --op dump-chunk-refs --chunk-pool default-cold-pool --object {}".format(cold_object_name))
    count = json.loads(json_data)["count"]
    refs_count = len(json.loads(json_data)["refs"])
    log("count: {}, refs_count: {}".format(count, refs_count))

    if count >= MAX_CHUNK_REF_SIZE - 10 and count <= MAX_CHUNK_REF_SIZE + 10 and refs_count >= MAX_CHUNK_REF_SIZE - 10 and refs_count <= MAX_CHUNK_REF_SIZE + 10:
        print("TEST06 passed!")
    else:
        print("TEST06 failed!")
        TEST_SUCCESS = False

    delete_rgw_object(bucket_name, file_name)
    delete_file(file_name)
    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 07. FPManager entry eviction when fingerprint entries exceed the memory target
def test07():
    global TEST_SUCCESS

    # change fpmanager_memory target to 4MB
    original_fpmanager_memory_target = int(os.popen("ceph config get client.rgw rgw_dedup_fpmanager_memory_limit").read())
    log("original_fpmanager_memory target: {}".format(original_fpmanager_memory_target))

    os.system("ceph config set client.rgw rgw_dedup_fpmanager_memory_limit 4194304")
    log("set fpmanager_memory target to 4MB")
    log("cmd: ceph config set client.rgw rgw_dedup_fpmanager_memory_limit 4194304")

    rgw_service_name = os.popen("ceph orch ls | grep rgw. | grep -v ingress | awk '{print $1}'").read().strip()
    log("rgw_service_name: {}".format(rgw_service_name))

    os.system("ceph orch restart {}".format(rgw_service_name))
    log("cmd: ceph orch restart {}".format(rgw_service_name))
    time.sleep(60)


    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    file_count = 10
    file_mode = "random"
    file_name = "testfile"
    file_size = "2G"
    for i in range(file_count):
        create_file(file_mode, file_name + str(i), file_size)
        upload_rgw_object(bucket_name, file_name + str(i))

    time.sleep(300)

    chunk_file_mode = "zero"
    chunk_file_name = "testchunkfile"
    chunk_file_size = "4M"
    create_file(chunk_file_mode, chunk_file_name, chunk_file_size)
    upload_rgw_object(bucket_name, chunk_file_name)

    time.sleep(300)

    if stat_rados_object(COLD_POOL, "1adc95bebe9eea8c112d40cd04ab7a8d75c4f961"):
        print("TEST07 passed!")
    else:
        print("TEST07 failed!")
        TEST_SUCCESS = False

    for i in range(file_count):
        delete_rgw_object(bucket_name, file_name + str(i))
        delete_file(file_name + str(i))
    delete_rgw_object(bucket_name, chunk_file_name)
    delete_file(chunk_file_name)
    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()

    # revert fpmanager_memory target config to original value
    os.system("ceph config set client.rgw rgw_dedup_fpmanager_memory_limit {}".format(original_fpmanager_memory_target))
    log("set fpmanager_memory target to original_fpmanager_memory_target")
    log("cmd: ceph config set client.rgw rgw_dedup_fpmanager_memory_limit {}".format(original_fpmanager_memory_target))

    os.system("ceph orch restart {}".format(rgw_service_name))
    log("cmd: ceph orch restart {}".format(rgw_service_name))
    time.sleep(60)


# 08. scrub mismatched chunk
def test08():
    bucket_name = "testbucket"
    file_mode = "random-with-hole"
    file_name = "testfile"
    file_size = "1M"

    create_rgw_bucket(bucket_name)
    
    create_file(file_mode, file_name, file_size)

    upload_rgw_object(bucket_name, file_name)

    while True:
        if len(list_rados_pool(COLD_POOL)) == 0:
            log("waiting for cold object creation...")
            time.sleep(1)
        elif len(list_rados_pool(COLD_POOL)) == 1:
            cold_object_name = list_rados_pool(COLD_POOL)[0]
            log("cold object created, cold object name: {}".format(cold_object_name))
            break
        else:
            raise Exception("cold object more than 1, expected: 1, actual: {}".format(len(list_rados_pool(COLD_POOL))))
    
    log("cmd: ceph-dedup-tool --op dump-chunk-refs --chunk-pool {} --object {} | grep count | awk '{{print $2}}'".format(COLD_POOL, cold_object_name))
    cold_object_ref_count = os.popen("ceph-dedup-tool --op dump-chunk-refs --chunk-pool {} --object {} | grep count | awk '{{print $2}}'".format(COLD_POOL, cold_object_name)).read().strip()[:-1]
    log("cold object ref count: {}".format(cold_object_ref_count))

    # use dummy file to make reference mismatch
    dummy_file_mode = "zero"
    dummy_file_name = "testdummyfile"
    dummy_file_size = "1"
    create_file(dummy_file_mode, dummy_file_name, dummy_file_size)

    upload_rados_object(BASE_POOL, dummy_file_name)
    
    stat_rados_object(BASE_POOL, dummy_file_name)

    log("cmd: ceph osd pool ls detail | grep {} | awk '{{print $2}}'".format(BASE_POOL))
    base_pool_id = os.popen("ceph osd pool ls detail | grep {} | awk '{{print $2}}'".format(BASE_POOL)).read().strip()
    log("base pool id: {}".format(base_pool_id))

    log("cmd: ceph-dedup-tool --op chunk-get-ref --chunk-pool {} --object {} --target-ref-pool-id {} --target-ref {}".format(COLD_POOL, cold_object_name, base_pool_id, dummy_file_name))
    os.system("ceph-dedup-tool --op chunk-get-ref --chunk-pool {} --object {} --target-ref-pool-id {} --target-ref {}".format(COLD_POOL, cold_object_name, base_pool_id, dummy_file_name))

    log("cmd: ceph-dedup-tool --op dump-chunk-refs --chunk-pool {} --object {} | grep count | awk '{{print $2}}'".format(COLD_POOL, cold_object_name))
    cold_object_ref_count_after = os.popen("ceph-dedup-tool --op dump-chunk-refs --chunk-pool {} --object {} | grep count | awk '{{print $2}}'".format(COLD_POOL, cold_object_name)).read().strip()[:-1]
    log("cold_object_ref_count_after: {}".format(cold_object_ref_count_after))
    
    if int(cold_object_ref_count_after) != int(cold_object_ref_count) + 1:
        raise Exception("cold object ref count is not increased, before: {}, after: {}".format(cold_object_ref_count, cold_object_ref_count_after))
    
    # check reference mismatch fixed with scrubbing
    start_time = time.time()
    while True:
        log("cmd: ceph-dedup-tool --op dump-chunk-refs --chunk-pool {} --object {} | grep count | awk '{{print $2}}'".format(COLD_POOL, cold_object_name))
        cold_object_ref_count_after = os.popen("ceph-dedup-tool --op dump-chunk-refs --chunk-pool {} --object {} | grep count | awk '{{print $2}}'".format(COLD_POOL, cold_object_name)).read().strip()[:-1]
        log("cold_object_ref_count_after: {}".format(cold_object_ref_count_after))

        if int(cold_object_ref_count_after) == int(cold_object_ref_count):
            log("now scrub finished, cold object ref count is {}".format(cold_object_ref_count_after))
            print("TEST08 passed!")
            break
        elif time.time() - start_time > 300:
            print("TEST08 failed!")
            TEST_SUCCESS = False
            break
        else:
            log("waiting for cold object ref count to be {}, cold object ref count: {}".format(cold_object_ref_count, cold_object_ref_count_after))
            time.sleep(5)

    delete_rgw_object(bucket_name, file_name)
    delete_file(file_name)
    delete_rados_object(BASE_POOL, dummy_file_name)
    delete_file(dummy_file_name)
    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 09. check dedup_worker thread count in RGW node
def test09():
    global TEST_SUCCESS
    
    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    file_count = 100
    file_mode = "random"
    file_name = "testfile"
    file_size = "100M"
    for i in range(file_count):
        create_file(file_mode, file_name + str(i), file_size)
        upload_rgw_object(bucket_name, file_name + str(i))

    cmd = "sshpass -pRoot0215! ssh root@{} ps -eLa | grep DedupWorker | wc -l".format(RGW_VIRTUAL_IP)
    log("cmd: {}".format(cmd))
    dedup_worker_total_thread = int(os.popen(cmd).read().strip())
    log("dedup_worker_total_thread: {}".format(dedup_worker_total_thread))

    if dedup_worker_total_thread == RGW_INSTANCE_NUM * DEDUP_WORKER_THREAD_NUM:
        print("TEST09 passed!")
    else:
        print("TEST09 failed!")
        TEST_SUCCESS = False

    # 파일 삭제
    for i in range(file_count):
        delete_rgw_object(bucket_name, file_name + str(i))
        delete_file(file_name + str(i))
    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 10. data consistency when Restart RGW instance while dedup
def test10():
    global TEST_SUCCESS

    bucket_name = "testbucket"

    create_rgw_bucket(bucket_name)

    thread_count = 100
    file_count = 100
    file_mode = "random"
    file_size = "10M"
    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=create_and_upload_file_thread, args=(i, bucket_name, file_count, file_mode, file_size))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    
    rgw_service_name = os.popen("ceph orch ls | grep rgw. | grep -v ingress | awk '{print $1}'").read().strip()
    log("rgw_service_name: {}".format(rgw_service_name))
    rgw_service_num = int(os.popen("ceph orch ls | grep rgw. | grep -v ingress | awk '{print $3}'").read().strip()[0])
    log("rgw_service_num: {}".format(rgw_service_num))

    # stop rgw instances
    os.system("ceph orch stop {}".format(rgw_service_name))

    # waiting for rgw instances to be stop
    start_time = time.time()
    while True:
        cmd = "ceph orch ls | grep rgw. | grep -v ingress | awk '{print $3}'"
        log("cmd: {}".format(cmd))
        rgw_running = int(os.popen(cmd).read().strip()[0])
        log("rgw_running: {}".format(rgw_running))

        if rgw_running == 0:
            log("rgw_running: {}".format(rgw_running))
            break
        elif time.time() - start_time > 300:
            raise Exception("rgw instance stop timeout")
        else:
            log("waiting for rgw instance to be stop, now: {}".format(rgw_running))
            time.sleep(5)

    time.sleep(10)

    # start rgw instances
    cmd = "ceph orch start {}".format(rgw_service_name)
    log("cmd: {}".format(cmd))
    os.system(cmd)

    # waiting for rgw instances to be start
    start_time = time.time()
    while True:
        cmd = "ceph orch ls | grep rgw. | grep -v ingress | awk '{print $3}'"
        log("cmd: {}".format(cmd))
        rgw_running = int(os.popen(cmd).read().strip()[0])
        log("rgw_running: {}".format(rgw_running))

        if rgw_running == rgw_service_num:
            log("rgw_running: {}".format(rgw_running))
            break
        elif time.time() - start_time > 300:
            raise Exception("rgw instance stop timeout")
        else:
            log("waiting for rgw instance to be start, now: {}".format(rgw_running))
            time.sleep(5)

    time.sleep(300)

    global THREAD_SUCCESS
    THREAD_SUCCESS = True

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=download_and_compare_file_thread, args=(i, bucket_name, file_count))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    if THREAD_SUCCESS:
        print("TEST10 passed!")
    else:
        print("TEST10 failed!")
        TEST_SUCCESS = False

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=delete_file_thread, args=(i, bucket_name, file_count))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 11. object resharding when RGW instance failure
def test11():
    global TEST_SUCCESS

    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    thread_count = 100
    file_count = 100
    file_mode = "random"
    file_size = "10M"

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=create_and_upload_file_thread, args=(i, bucket_name, file_count, file_mode, file_size))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    
    rgw_service_name = os.popen("ceph orch ls | grep rgw. | grep -v ingress | awk '{print $1}'").read().strip()
    log("rgw_service_name: {}".format(rgw_service_name))
    rgw_service_num = int(os.popen("ceph orch ls | grep rgw. | grep -v ingress | awk '{print $3}'").read().strip()[0])
    log("rgw_service_num: {}".format(rgw_service_num))

    # stop 1 rgw instance
    cmd = "ceph orch apply rgw {} \"--placement={} {}\" --port=8080".format(rgw_service_name.split(".")[1], rgw_service_num - 1, RGW_NODE_HOSTNAME)
    log("cmd: {}".format(cmd))
    os.system(cmd)

    start_time = time.time()
    while True:
        cmd = "ceph orch ls | grep rgw. | grep -v ingress | awk '{print $3}'"
        log("cmd: {}".format(cmd))
        rgw_running = int(os.popen(cmd).read().strip()[0])
        log("rgw_running: {}".format(rgw_running))

        if rgw_running == rgw_service_num - 1:
            log("rgw_running: {}".format(rgw_running))
            break
        elif time.time() - start_time > 300:
            raise Exception("rgw instance apply timeout")
        else:
            log("waiting for rgw instance to be apply, now: {}".format(rgw_running))
            time.sleep(5)

    time.sleep(10)

    cmd = "sshpass -pRoot0215! ssh root@{} ps -eLa | grep DedupWorker | wc -l".format(RGW_VIRTUAL_IP)
    log("cmd: {}".format(cmd))
    dedup_worker_total_thread = int(os.popen(cmd).read().strip())
    log("dedup_worker_total_thread: {}".format(dedup_worker_total_thread))

    if dedup_worker_total_thread == DEDUP_WORKER_THREAD_NUM * (rgw_service_num - 1):
        log("dedup_worker_total_thread: {}".format(dedup_worker_total_thread))
        print("TEST11 passed!")
    else:
        print("TEST11 failed!")

    # recover rgw instance
    cmd = "ceph orch apply rgw {} \"--placement={} {}\" --port=8080".format(rgw_service_name.split(".")[1], rgw_service_num, RGW_NODE_HOSTNAME)
    log("cmd: {}".format(cmd))
    os.system(cmd)

    start_time = time.time()
    while True:
        cmd = "ceph orch ls | grep rgw. | grep -v ingress | awk '{print $3}'"
        log("cmd: {}".format(cmd))
        rgw_running = int(os.popen(cmd).read().strip()[0])
        log("rgw_running: {}".format(rgw_running))

        if rgw_running == rgw_service_num:
            log("rgw_running: {}".format(rgw_running))
            break
        elif time.time() - start_time > 300:
            raise Exception("rgw instance apply timeout")
        else:
            log("waiting for rgw instance to be apply, now: {}".format(rgw_running))
            time.sleep(5)
    time.sleep(10)

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=delete_file_thread, args=(i, bucket_name, file_count))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 12. Heavy load(object read/write)
def test12():
    global TEST_SUCCESS
    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    thread_count = 100
    file_count = 100
    file_mode = "random"
    file_size = "10M"

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=create_and_upload_file_thread, args=(i, bucket_name, file_count, file_mode, file_size))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    time.sleep(60)
    
    read_thread_count = 100
    write_thread_count = 100
    read_file_count = 100
    write_file_count = 100
    
    # load prepare
    read_threads = []
    write_threads = []
    
    for i in range(read_thread_count):
        rt = threading.Thread(target=read_load_prepare_thread, args=(i, bucket_name, read_file_count))
        read_threads.append(rt)
        rt.start()

    for rt in read_threads:
        rt.join()

    for i in range(write_thread_count):
        wt = threading.Thread(target=write_load_prepare_thread, args=(i, write_file_count))
        write_threads.append(wt)
        wt.start()

    for wt in write_threads:
        wt.join()

    # load start
    read_threads = []
    for i in range(read_thread_count):
        rt = threading.Thread(target=read_load_thread, args=(i, bucket_name, read_file_count))
        read_threads.append(rt)
        rt.start()

    write_threads = []
    for i in range(write_thread_count):
        wt = threading.Thread(target=write_load_thread, args=(i, bucket_name, write_file_count))
        write_threads.append(wt)
        wt.start()

    time.sleep(100)

    global THREAD_SUCCESS
    THREAD_SUCCESS = True

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=download_and_compare_file_thread, args=(i, bucket_name, file_count))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    if THREAD_SUCCESS:
        print("TEST12 passed!")
    else:
        print("TEST12 failed!")

    # load stop
    global THREAD_EXIT_SIGNAL
    THREAD_EXIT_SIGNAL = True

    for rt in read_threads:
        rt.join()

    for wt in write_threads:
        wt.join()

    read_threads = []
    for i in range(read_thread_count):
        rt = threading.Thread(target=delete_read_load_thread, args=(i, bucket_name, read_file_count))
        read_threads.append(rt)
        rt.start()

    for rt in read_threads:
        rt.join()

    write_threads = []
    for i in range(write_thread_count):
        wt = threading.Thread(target=delete_write_load_thread, args=(i, bucket_name, write_file_count))
        write_threads.append(wt)
        wt.start()

    for wt in write_threads:
        wt.join()

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=delete_file_thread, args=(i, bucket_name, file_count))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# 13. big object consistency when RGW restart
def test13():
    global TEST_SUCCESS

    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    thread_count = 10
    file_count = 1
    file_mode = "big-random"
    file_size = "10G"

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=create_and_upload_file_thread, args=(i, bucket_name, file_count, file_mode, file_size))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    time.sleep(60)

    rgw_service_name = os.popen("ceph orch ls | grep rgw. | grep -v ingress | awk '{print $1}'").read().strip()
    log("rgw_service_name: {}".format(rgw_service_name))
    rgw_service_num = int(os.popen("ceph orch ls | grep rgw. | grep -v ingress | awk '{print $3}'").read().strip()[0])
    log("rgw_service_num: {}".format(rgw_service_num))

    cmd = "ceph orch stop {}".format(rgw_service_name)
    log("cmd: {}".format(cmd))
    os.system(cmd)

    start_time = time.time()
    while True:
        rgw_running = int(os.popen("ceph orch ls | grep rgw. | grep -v ingress | awk '{print $3}'").read().strip()[0])
        if rgw_running == 0:
            log("all rgw instance stopped")
            break
        elif time.time() - start_time > 300:
            log("rgw stop timeout")
            break
        else:
            log("waiting for rgw stop, current rgw instance num: {}/{}".format(rgw_running, rgw_service_num))
            time.sleep(5)

    time.sleep(60)

    cmd = "ceph orch start {}".format(rgw_service_name)
    log("cmd: {}".format(cmd))
    os.system(cmd)

    start_time = time.time()
    while True:
        rgw_running = int(os.popen("ceph orch ls | grep rgw. | grep -v ingress | awk '{print $3}'").read().strip()[0])
        if rgw_running == rgw_service_num:
            log("all rgw instance started")
            break
        elif time.time() - start_time > 300:
            log("rgw start timeout")
            break
        else:
            log("waiting for rgw start, current rgw instance num: {}/{}".format(rgw_running, rgw_service_num))
            time.sleep(5)

    time.sleep(60)

    global THREAD_SUCCESS
    THREAD_SUCCESS = True
    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=download_and_compare_file_thread, args=(i, bucket_name, file_count))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    if THREAD_SUCCESS:
        print("TEST13 passed!")
    else:
        print("TEST13 failed!")
        TEST_SUCCESS = False

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=delete_file_thread, args=(i, bucket_name, file_count))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()
    

# 14. round sync when lopsided load without HAProxy
def test14():
    global TEST_SUCCESS

    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    thread_count = 100
    file_count = 100
    file_mode = "random"
    file_size = "10M"

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=create_and_upload_file_thread, args=(i, bucket_name, file_count, file_mode, file_size))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    read_thread_count = 100
    write_thread_count = 100
    read_file_count = 100
    write_file_count = 100
    
    read_threads = []
    for i in range(read_thread_count):
        rt = threading.Thread(target=read_load_prepare_thread, args=(i, bucket_name, read_file_count))
        read_threads.append(rt)
        rt.start()

    for rt in read_threads:
        rt.join()

    write_threads = []
    for i in range(write_thread_count):
        wt = threading.Thread(target=write_load_prepare_thread, args=(i, write_file_count))
        write_threads.append(wt)
        wt.start()

    for wt in write_threads:
        wt.join()

    global RGW_VIRTUAL_IP
    RGW_HOST_WITH_HAPROXY = RGW_VIRTUAL_IP
    RGW_VIRTUAL_IP = RGW_NODE_IP + ":8080"

    read_threads = []
    for i in range(read_thread_count):
        rt = threading.Thread(target=read_load_thread, args=(i, bucket_name, read_file_count))
        read_threads.append(rt)
        rt.start()
    
    write_threads = []
    for i in range(write_thread_count):
        wt = threading.Thread(target=write_load_thread, args=(i, bucket_name, write_thread_count))
        write_threads.append(wt)
        wt.start()

    time.sleep(60)

    cmd = "sshpass -pRoot0215! ssh root@{} ps -eLa | grep DedupWorker | wc -l".format(RGW_HOST_WITH_HAPROXY)
    log("cmd: {}".format(cmd))
    dedup_worker_total_thread = int(os.popen(cmd).read().strip())
    log("dedup_worker_total_thread: {}".format(dedup_worker_total_thread))

    if dedup_worker_total_thread == RGW_INSTANCE_NUM * DEDUP_WORKER_THREAD_NUM:
        print("TEST14 passed!")
    else:
        print("TEST14 failed!")
        TEST_SUCCESS = False


    global THREAD_EXIT_SIGNAL
    THREAD_EXIT_SIGNAL = True

    for rt in read_threads:
        rt.join()

    for wt in write_threads:
        wt.join()

    RGW_VIRTUAL_IP = RGW_HOST_WITH_HAPROXY

    read_threads = []
    for i in range(read_thread_count):
        rt = threading.Thread(target=delete_read_load_thread, args=(i, bucket_name, read_file_count))
        read_threads.append(rt)
        rt.start()

    for rt in read_threads:
        rt.join()

    write_threads = []
    for i in range(write_thread_count):
        wt = threading.Thread(target=delete_write_load_thread, args=(i, bucket_name, write_file_count))
        write_threads.append(wt)
        wt.start()

    for wt in write_threads:
        wt.join()

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=delete_file_thread, args=(i, bucket_name, file_count))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    delete_rgw_bucket
    clear_rgw()
    clear_cold_pool()


# 15. dedup data consistency during flapping OSD
def test15():
    global TEST_SUCCESS

    bucket_name = "testbucket"
    create_rgw_bucket(bucket_name)

    thread_count = 100
    file_count = 100
    file_mode = "random"
    file_size = "10M"

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=create_and_upload_file_thread, args=(i, bucket_name, file_count, file_mode, file_size))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    cmd = "ceph orch daemon stop osd.0"
    log("cmd: {}".format(cmd))
    os.system(cmd)
    time.sleep(3)

    cmd = "ceph orch daemon stop osd.1"
    log("cmd: {}".format(cmd))
    os.system(cmd)
    time.sleep(3)

    cmd = "ceph orch daemon stop osd.2"
    log("cmd: {}".format(cmd))
    os.system(cmd)
    time.sleep(3)

    for i in range(10):
        cmd = "ceph orch daemon stop osd.3"
        log("cmd: {}".format(cmd))
        os.system(cmd)
        time.sleep(10)
        cmd = "ceph orch daemon stop osd.4"
        log("cmd: {}".format(cmd))
        os.system(cmd)
        time.sleep(10)
        cmd = "ceph orch daemon stop osd.5"
        log("cmd: {}".format(cmd))
        os.system(cmd)
        time.sleep(10)
        cmd = "ceph orch daemon start osd.3"
        log("cmd: {}".format(cmd))
        os.system(cmd)
        time.sleep(10)
        cmd = "ceph orch daemon start osd.4"
        log("cmd: {}".format(cmd))
        os.system(cmd)
        time.sleep(10)
        cmd = "ceph orch daemon start osd.5"
        log("cmd: {}".format(cmd))
        os.system(cmd)
        time.sleep(10)

    cmd = "ceph orch daemon start osd.0"
    log("cmd: {}".format(cmd))
    os.system(cmd)
    time.sleep(10)

    cmd = "ceph orch daemon start osd.1"
    log("cmd: {}".format(cmd))
    os.system(cmd)
    time.sleep(10)

    cmd = "ceph orch daemon start osd.2"
    log("cmd: {}".format(cmd))
    os.system(cmd)
    time.sleep(10)

    global THREAD_SUCCESS
    THREAD_SUCCESS = True
    
    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=download_and_compare_file_thread, args=(i, bucket_name, file_count))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    if THREAD_SUCCESS:
        print("TEST15 passed!")
    else:
        print("TEST15 failed!")
        TEST_SUCCESS = False

    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=delete_file_thread, args=(i, bucket_name, file_count))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    
    delete_rgw_bucket(bucket_name)
    clear_rgw()
    clear_cold_pool()


# main 함수
if __name__ == "__main__":
    if len(sys.argv) < 22 or "-h" in sys.argv or "--help" in sys.argv:
        print("Usage: python3 rgw_dedup_integration_test.py [image name] " \
                "[bootstrap node ip] [bootstrap node pw] " \
                "[osd node 1 ip] [osd node 1 pw] [osd node 1 disk 1] [osd node 1 disk 2] [osd node 1 disk 3] " \
                "[osd node 2 ip] [osd node 2 pw] [osd node 2 disk 1] [osd node 2 disk 2] [osd node 2 disk 3] " \
                "[osd node 3 ip] [osd node 3 pw] [osd node 3 disk 1] [osd node 3 disk 2] [osd node 3 disk 3] " \
                "[rgw node ip] [rgw node pw] [rgw virtual ip] " \
                "[-v / --verbose]")
        sys.exit(1)
    
    if "-v" in sys.argv or "--verbose" in sys.argv:
        VERBOSE = True

    IMAGE_NAME = sys.argv[1]
    BOOTSTRAP_NODE_IP = sys.argv[2]
    BOOTSTRAP_NODE_PW = sys.argv[3]
    OSD_NODE_1_IP = sys.argv[4]
    OSD_NODE_1_PW = sys.argv[5]
    OSD_NODE_1_DISK_1 = sys.argv[6]
    OSD_NODE_1_DISK_2 = sys.argv[7]
    OSD_NODE_1_DISK_3 = sys.argv[8]
    OSD_NODE_2_IP = sys.argv[9]
    OSD_NODE_2_PW = sys.argv[10]
    OSD_NODE_2_DISK_1 = sys.argv[11]
    OSD_NODE_2_DISK_2 = sys.argv[12]
    OSD_NODE_2_DISK_3 = sys.argv[13]
    OSD_NODE_3_IP = sys.argv[14]
    OSD_NODE_3_PW = sys.argv[15]
    OSD_NODE_3_DISK_1 = sys.argv[16]
    OSD_NODE_3_DISK_2 = sys.argv[17]
    OSD_NODE_3_DISK_3 = sys.argv[18]
    RGW_NODE_IP = sys.argv[19]
    RGW_NODE_PW = sys.argv[20]
    RGW_VIRTUAL_IP = sys.argv[21]

    destroy_ceph()
    bootstrap_ceph()
    preflight_check()
    
    test01()
    test02()
    test03()
    test04()
    test05()
    test06()
    test07()
    test08()
    test09()
    test10()
    test11()
    test12()
    test13()
    test14()
    test15()

    destroy_ceph()

    if TEST_SUCCESS :
        print("All tests passed!")
        sys.exit(0)
    else:
        print("Some tests failed!")
        sys.exit(1)