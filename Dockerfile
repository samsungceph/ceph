# CEPH DAEMON BASE IMAGE

FROM ubuntu:jammy

ADD ./ceph/ /root/debs

ENV I_AM_IN_A_CONTAINER 1

# Who is the maintainer ?
LABEL maintainer=""

# Is a ceph container ?
LABEL ceph="True"

# What is the actual release ? If not defined, this equals the git branch name
LABEL RELEASE="main"

# What was the url of the git repository
LABEL GIT_REPO="https://github.com/canonical/ceph-container.git"

# What was the git branch used to build this container
LABEL GIT_BRANCH="main"

# What was the commit ID of the current HEAD
ARG GIT_COMMIT=unspecified
LABEL git_commit=$GIT_COMMIT

# Was the repository clean when building ?
LABEL GIT_CLEAN="True"

# What CEPH_POINT_RELEASE has been used ?
LABEL CEPH_POINT_RELEASE="-17.2.0"

ENV CEPH_VERSION quincy
ENV CEPH_POINT_RELEASE "-17.2.0"
ENV CEPH_DEVEL false
ENV CEPH_REF quincy
ENV OSD_FLAVOR default

# Additional custom .deb repo
ARG CUSTOM_APT_REPO=""


#======================================================
# Install ceph and dependencies, and clean up
#======================================================

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
DEBIAN_FRONTEND=noninteractive apt-get install -yy --force-yes --no-install-recommends \
gnupg curl apt-transport-https ca-certificates

# Optionally inject custom apt repo
RUN if [ -n "${CUSTOM_APT_REPO}" ] ; then \
      DEBIAN_FRONTEND=noninteractive apt-get install -yy --no-install-recommends gpg-agent software-properties-common ; \
      DEBIAN_FRONTEND=noninteractive add-apt-repository -y "${CUSTOM_APT_REPO}" ; \
    fi

# Escape char after immediately after RUN allows comment in first line
RUN \
    # Install all components for the image, whether from packages or web downloads.
    # Typical workflow: add new repos; refresh repos; install packages; package-manager clean;
    #   download and install packages from web, cleaning any files as you go.
    # Installs should support install of ganesha for luminous
    # add the necessary repos
    echo "" > /etc/apt/sources.list && \
    echo "deb http://archive.ubuntu.com/ubuntu/ jammy-backports main" \
      >> /etc/apt/sources.list.d/erp.list && \
    echo "deb http://archive.ubuntu.com/ubuntu/ jammy main universe multiverse" \
      >> /etc/apt/sources.list.d/jammy.list && \
    echo "deb http://archive.ubuntu.com/ubuntu/ jammy-updates main universe multiverse" \
      >> /etc/apt/sources.list.d/jammy.list && \
    DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -yy --force-yes /root/debs/*.deb && \
    DEBIAN_FRONTEND=noninteractive apt-get install -yy --force-yes --no-install-recommends \
       \
        vim bash-completion wget curl nmap \
        ca-certificates \
        e2fsprogs \
        ceph-common  \
        ceph-mon  \
        ceph-osd \
        ceph-mds \
        rbd-mirror  \
        ceph-mgr \
ceph-mgr-cephadm \
ceph-mgr-dashboard \
ceph-mgr-diskprediction-local \
ceph-mgr-k8sevents \
ceph-mgr-rook\
        ceph-grafana-dashboards \
        kmod \
        lvm2 \
        gdisk \
	smartmontools \
	nvme-cli \
        radosgw \
        nfs-ganesha nfs-ganesha-ceph \
        ceph-iscsi targetcli-fb \
        attr \
ceph-fuse \
rbd-nbd \
cephfs-mirror \
         && \
    # Clean container, starting with record of current size (strip / from end)
    INITIAL_SIZE="$(bash -c 'sz="$(du -sm --exclude=/proc /)" ; echo "${sz%*/}"')" && \
    #
    #
    # Perform any final cleanup actions like package manager cleaning, etc.
    echo 'Postinstall cleanup' && \
    #  ( echo "apt clean" && DEBIAN_FRONTEND=noninteractive apt-get clean && \
    #   echo "apt autoclean" && DEBIAN_FRONTEND=noninteractive apt-get autoclean ) || \
    #   ( retval=$? && cat /var/log/apt/history.log && exit $retval ) && \
    # echo 'remove unneeded apt, deb, dpkg data' && \
    #   rm -rf /var/lib/apt/lists/* \
    #          /var/cache/debconf/* \
    #          /var/log/apt/ \
    #          /var/log/dpkg.log \ 
    #          /tmp/* && \
    # /bin/true && \
    # Tweak some configuration files on the container system
    # disable sync with udev since the container can not contact udev
sed -i -e 's/udev_rules = 1/udev_rules = 0/' -e 's/udev_sync = 1/udev_sync = 0/' -e 's/obtain_device_list_from_udev = 1/obtain_device_list_from_udev = 0/' /etc/lvm/lvm.conf && \
# validate the sed command worked as expected
grep -sqo "udev_sync = 0" /etc/lvm/lvm.conf && \
grep -sqo "udev_rules = 0" /etc/lvm/lvm.conf && \
grep -sqo "obtain_device_list_from_udev = 0" /etc/lvm/lvm.conf && \
mkdir -p /var/run/ceph /var/run/ganesha && \
    # Clean common files like /tmp, /var/lib, etc.
    rm -rf \
        /etc/{selinux,systemd,udev} \
        /lib/{lsb,udev} \
        /tmp/* \
        /usr/lib{,64}/{locale,systemd,udev,dracut} \
        /usr/share/{doc,info,locale,man} \
        /var/log/* \
        /var/tmp/* && \
    find  / -xdev -name "*.pyc" -o -name "*.pyo" -exec rm -f {} \; && \
    # ceph-dencoder is only used for debugging, compressing it saves 10MB
    # If needed it will be decompressed
    # TODO: Is ceph-dencoder safe to remove as rook was trying to do?
    # rm -f /usr/bin/ceph-dencoder && \
    if [ -f /usr/bin/ceph-dencoder ]; then gzip -9 /usr/bin/ceph-dencoder; fi && \
    # TODO: What other ceph stuff needs removed/stripped/zipped here?
    # Photoshop files inside a container ?
    rm -f /usr/lib/ceph/mgr/dashboard/static/AdminLTE-*/plugins/datatables/extensions/TableTools/images/psd/* && \
    # Some logfiles are not empty, there is no need to keep them
    find /var/log/ -type f -exec truncate -s 0 {} \; && \
    #
    #
    # Report size savings (strip / from end)
    FINAL_SIZE="$(bash -c 'sz="$(du -sm --exclude=/proc /)" ; echo "${sz%*/}"')" && \
    REMOVED_SIZE=$((INITIAL_SIZE - FINAL_SIZE)) && \
    echo "Cleaning process removed ${REMOVED_SIZE}MB" && \
    echo "Dropped container size from ${INITIAL_SIZE}MB to ${FINAL_SIZE}MB" && \
    #
    # Verify that the packages installed haven't been accidentally cleaned
    apt-cache show \
        ca-certificates \
        e2fsprogs \
        ceph-common  \
        ceph-mon  \
        ceph-osd \
        ceph-mds \
        rbd-mirror  \
        ceph-mgr \
ceph-mgr-cephadm \
ceph-mgr-dashboard \
ceph-mgr-diskprediction-local \
ceph-mgr-k8sevents \
ceph-mgr-rook\
        ceph-grafana-dashboards \
        kmod \
        lvm2 \
        gdisk \
	smartmontools \
	nvme-cli \
        radosgw \
        nfs-ganesha nfs-ganesha-ceph \
        ceph-iscsi targetcli-fb \
        attr \
ceph-fuse \
rbd-nbd \
         && echo 'Packages verified successfully'

#======================================================
# Install ceph and dependencies, and clean up
#======================================================


# Escape char after immediately after RUN allows comment in first line
RUN \
    # Install all components for the image, whether from packages or web downloads.
    # Typical workflow: add new repos; refresh repos; install packages; package-manager clean;
    #   download and install packages from web, cleaning any files as you go.
    echo 'Install packages' && \
      DEBIAN_FRONTEND=noninteractive apt-get update && \
      DEBIAN_FRONTEND=noninteractive apt-get install -y \
        wget unzip uuid-runtime python-setuptools udev dmsetup ceph-volume python3-asyncssh python3-natsort && \
      apt-get install -y  --no-install-recommends --force-yes \
          sharutils \
          lsof \
           \
           \
          etcd-client \
          s3cmd

