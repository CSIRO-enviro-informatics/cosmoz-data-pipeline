#!/bin/bash
#singularity-run.sh
if [ $EUID != 0 ]; then
    echo "This singularity orchestration script MUST be run as superuser."
    echo "Elevating previleges."
    echo "If requested, please input your sudo password, or press ctrl+c to cancel."
    sudo "$0" "$@"
    exit $?
fi
if [ ! -f ./alpine39.sif ]; then
    echo "Building alpine container image."
    sudo singularity build alpine39.sif library://alpine:3.9
fi

echo "First trying to stop existing cosmoz mongodb container"
sudo singularity instance stop t1
sudo singularity instance start --writable-tmpfs --net --network bridge --hostname "t1" alpine39.sif t1
T1_IP_ADDR="$(sudo singularity exec instance://t1 ip addr|awk -F'[ \n\t/]+' '/global/ { print $3 }')"
echo $T1_IP_ADDR
echo "First trying to stop existing cosmoz influxdb container"
sudo singularity instance stop t2
sudo singularity instance start --writable-tmpfs --net --network bridge --hostname "t2" alpine39.sif t2
T2_IP_ADDR="$(sudo singularity exec instance://t2 ip addr|awk -F'[ \n\t/]+' '/global/ { print $3 }')"
echo $T2_IP_ADDR
echo "First trying to stop existing cosmoz influxdb container"
sudo singularity instance stop t3
sudo rm -f ./dummy_hosts.txt
echo "127.0.0.1       localhost" > ./dummy_hosts.txt
echo "127.0.1.1       local.cosmoz.csiro.au      local" >> ./dummy_hosts.txt
echo "$T2_IP_ADDR   t2.cosmoz.csiro.au      t2" >> ./dummy_hosts.txt
echo "$T1_IP_ADDR   t1.cosmoz.csiro.au      t1" >> ./dummy_hosts.txt
sudo singularity instance start --writable-tmpfs --net --network bridge --hostname "t3" --bind ./dummy_hosts.txt:/etc/hosts alpine39.sif t3
sudo singularity exec instance://t3 cat /etc/hosts
sudo singularity exec instance://t3 ping t1
#sudo singularity exec instance://t3 ping $T2_IP_ADDR


#sudo singularity exec instance://t1 ping google.com



