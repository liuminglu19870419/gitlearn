#!/bin/bash
echo $PATH | grep .
if [ $? -ne 0]
then
    PATH=$PATH:.
fi
export PATH
i=1
while [ $i -lt 31 ]
do
    filename=${i}std
    tempname=$i
    i=$(($i+1))
    if [ $i -lt 11 ]
    then
        filename=0${filename}
        tempname=0$i
    fi
    echo $filename
    sudo userdel -r $filename
    sudo userdel -r $tempname 
done
