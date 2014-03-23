#!/bin/bash
filename=" "
read -p "input the file name:" filename
sed -n '/ignore/!p' "$filename" | sed -n '/error/p;/fatal/p'
