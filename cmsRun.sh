#!/bin/bash -x

export MLAS_DYNAMIC_CPU_ARCH=2

env

cmsRun -j FrameworkJobReport.xml -p PSet.py
