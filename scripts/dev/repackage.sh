#!/bin/bash

for file in `find . -type f -name '*.*'`; do
    echo $file
    sed -i "" 's/com\.twitter\.distributedlog/org.apache.distributedlog/' $file
done
