#!/bin/bash
sed -n '/special 1,2/{
    s/line/test/pi
    }' input
echo '==================================='
sed '/special 1,4/d' input
echo '==================================='
sed '1,$i\new line' input
echo '==================================='
sed '1,$a\new line' input
echo '==================================='
sed '3c\chang this line' input
echo '==================================='
sed '1,$y/abcdefghijklmnopqrstuvwxyz/ABCDEFGHIJKLMNOPQRSTUVWXYZ/' input
echo '==================================='
sed '1,2w output' input
echo '==================================='
sed '3r output' input
echo '==================================='
sed -n 's/line//gp' input 
echo '==================================='
sed '/special/{n; d}' input
echo '==================================='
sed -n 'N;s/special\nthis/this\nthis/gp' input
echo '==================================='
sed -n '/special/{
    p
    h
    n
    p
    g
    p
    }' input
echo '==================================='
sed -n '{
    1!G
    h
    $p
   } ' input
