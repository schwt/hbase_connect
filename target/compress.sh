#!/bin/sh
# -*- coding: utf8 -*-
 #Date:        2020-03-09 15:45:50
dir=$(cd $(dirname $0); pwd)

tar -zcvf data.tar.gz $1
