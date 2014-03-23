#!/bin/bash
read -p "input number1: " num1
#echo "$num1"| grep '^[0-9]*[0-9]$'
echo $num1| grep '^[0-9]*[0-9]$'
if [ $? -ne 0 ]
then
    echo 'no'
    exit 1
else
    echo 'yes'
fi
read -p "input number2: " num2
echo "$num2"| grep '^[0-9]*[0-9]$'
if [ $? -ne 0 ]
then
    echo 'no'
    exit 1
else
    echo `expr $num1 - $num2` 
    exit 0
fi
