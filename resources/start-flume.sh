#!/bin/bash

FLUME_CONF_DIR=/root/flume/conf

flume-ng agent \
	--conf ${FLUME_CONF_DIR}/ \
	--conf-file ${FLUME_CONF_DIR}/flume.conf \
	--name bot-filter \
	-Dflume.root.logger=INFO,console \
	-Xmx3g