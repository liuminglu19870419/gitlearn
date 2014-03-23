#!/bin/bash
echo "$PATH"|grep -q .
if [ $? -eq 0 ]
then
    echo 'PATH include current dir'
else
    PATH=$PATH:.
    export $PATH
    echo 'PATH not include current dir'
fi
path='/dev/&'
read -p "input the file name:" file
if [ -b $file -o -c $file ]
then
    echo 'this is a block equip'
    sudo cp $file $path
else
    echo 'this is not a block equip'
    exit 1
fi
exit 0
