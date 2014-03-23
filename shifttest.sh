#!/bin/bash
echo $#
while [ -n "$1" ]
do
    echo "$1"
    shift
done
if read -t 5 -s -p "input a number:" num
then
    echo "$num"
else
    echo "\n"
    echo "slow"
fi
