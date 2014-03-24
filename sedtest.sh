#!/bin/bash
echo '---------------------'
sed -n 's/test/trail/p' data
echo '---------------------'
sed -n 's/test/trail/gp' data
echo '---------------------'
sed -n 's/test/trail/2p' data
echo '---------------------'
sed -n '/^a/s/test/trail/2p' data
