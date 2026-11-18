#!/bin/bash

mission="2B"
crs="EPSG:32631"
srcdir="./tmp"

# $1: resolution
# $2: file name
function rescale_if_needed(){
  res=$(gdalinfo $2 | grep 'Pixel Size' | sed 's/.*(//g;s/,.*//g;s/-//g')
  if [ "$(echo $res'=='$1 | bc -l)" -ne "1" ] ; then
    echo "Rescaling $2"
    mv $2 $2.tmp
    gdal_translate -a_srs $crs -tr $1 $1 $2.tmp $2
    rm $2.tmp
  fi
}

# $1 wildcard
# $2 output name
function find_copy(){
  if [ "$(ls -1 ${srcdir}/*$1* | wc -l)" -gt "1" ] ; then 
    echo "MULTIPLE FILES FOUND FOR: ${srcdir}/*$1*" 1>&2
    for i in $(ls ${srcdir}/*$1* | sort) ; do echo $i 1>&2 ; done
    echo "TAKING FIRST" 1>&2
  fi
  cp $(ls ${srcdir}/*$1* | sort | head -n 1 ) ./$2
  echo $2
}

# get common data
cp /data/TERRASCOPE/morpho_v2/process_data/process_data_20191021/sentinel2/Templates/Sentinel${mission}_SR_ALL_ACRUNNER.csv ./input_response_curves_all.txt
cp /data/TERRASCOPE/morpho_v2/process_data/process_data_20191021/sentinel2/Templates/LUT/S${mission}_10m.bin ./input_lut_10m.bin
cp /data/TERRASCOPE/morpho_v2/process_data/process_data_20191021/sentinel2/Templates/LUT/S${mission}_all.bin ./input_lut_all.bin

# input top of atmosphare radiances/reflectances 
#  * radiances: all bands at 60m resolution and B02,B03,B04,B08 at 10m
#  * reflectances: presumably B09,B8A,B11 at 10m
# TODO: check reflectance bands order and if those are used or not
rescale_if_needed 60 $(find_copy scaled_radiance_all input_scaled_radiance_all.tif)
rescale_if_needed 10 $(find_copy scaled_radiance_10m input_scaled_radiance_10m.tif)
rescale_if_needed 60 $(find_copy reflectance_60m     input_reflectance_60m.tif)

# sza,vza,raa angles at 10m, single band files
rescale_if_needed 10 $(find_copy sza input_10m_sza.tif)
rescale_if_needed 10 $(find_copy vza input_10m_vza.tif)
rescale_if_needed 10 $(find_copy raa input_10m_raa.tif)

# elevation data
rescale_if_needed 10 $(find_copy dem input_dem_10m.tif)
rescale_if_needed 60 $(find_copy dem input_dem_60m.tif)

# water mask
# TODO: can this be overridden by constant 0?
rescale_if_needed 10 $(find_copy watermask input_watermask_10m.tif)
rescale_if_needed 60 $(find_copy watermask input_watermask_60m.tif)

# cloud mask
# TODO: can this be overridden by constant 0?
rescale_if_needed 10 $(find_copy cloudmask input_cloudmask_10m.tif)
rescale_if_needed 60 $(find_copy cloudmask input_cloudmask_60m.tif)

# atmospheric optical thickness
rescale_if_needed 10 $(find_copy aot input_aot_10m.tif )

