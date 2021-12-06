#!/bin/bash

ac_runner=$(which ac_runner)

$ac_runner watervapor.conf
gdal_translate -tr 10 10 output_watervapor.tif input_watervapor_10m.tif
rm output_watervapor_interpol.tif
rm output_watervapor_low.tif
rm output_watervapor_resize.tif

$ac_runner watcor.conf
