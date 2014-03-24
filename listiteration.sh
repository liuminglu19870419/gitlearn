#!/bin/bash
for file in `ls -lR | awk '/^d/{ print $9 }'`
do
    echo $file
done
