#!/bin/bash
echo $PATH | grep . 
if [ $? -eq 0 ]
then
    echo -n 
else
    PATH=$PATH:.
    export $PATH
fi
groupname='class1'
groupadd $groupname 
if [  $? -ne 0 ]
then
    echo 'can not create group: '$groupname
    exit 1
fi
for ((i=1;i<31;i++))
do
    username=${i}std
    if [ $i -lt 10 ]
    then
        username=0${username}
    fi
    echo add user: $username
    useradd -g $groupname $username
done
