#!/bin/bash

cd ..
# 打包当前分支的代码
zip -r tmp/em.zip ./easymirror -x "*.pyc" \
&& cp -rf tmp/em.zip /Volumes/Users/lamter/Documents/workplace/easymirror/ \
&& cd /Volumes/Users/lamter/Documents/workplace/easymirror/ \
&& unzip -o em.zip