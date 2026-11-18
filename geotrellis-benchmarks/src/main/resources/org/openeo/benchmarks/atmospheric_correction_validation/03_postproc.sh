#!/bin/bash

crs="EPSG:32631"

# where to put the results
mkdir results

# $1 file name
# $2 extent
function explode_tif_bands(){
  echo "TIF EXPLODE: $1"
  gdalwarp -te $2 $1 ./results/$1.cut.tif
  for i in $(gdalinfo ./results/$1.cut.tif | grep Band | grep Block | sed -e 's/ //g;s/Band\(.*\)Block.*/\1/'); do 
    ifile=./results/icor_${1}_${i}.tif
    [ -f $ifile ] && rm $ifile
    gdal_translate -a_srs $crs -co compress=deflate -b $i ./results/$1.cut.tif $ifile
  done
  rm ./results/$1.cut.tif
}

# $1 file name
# $2 extent
function explode_jp2_bands(){
  echo "JP2 EXPLODE: $1"
  iband="$(echo $1 | rev | cut -d / -f 2 | rev )_$(echo $1 | sed -E 's/.*(_B0[2348][._]).*/\1/g;s/\./_/g')"
  gdalwarp -te $2 $1 ./results/$iband.cut.tif
  for i in $(gdalinfo ./results/$iband.cut.tif | grep Band | grep Block | sed -e 's/ //g;s/Band\(.*\)Block.*/\1/'); do 
    ifile=./results/sen2cor_$i_${iband}.tif
    [ -f $ifile ] && rm $ifile
    gdal_translate -a_srs $crs -co compress=deflate -b $i ./results/$iband.cut.tif $ifile
  done
  rm ./results/$iband.cut.tif
}

# $1 file name
function explode_netcdf_bands(){
  echo "NETCDF EXPLODE: $1"
  for i in $(gdalinfo $1 | grep SUBDATASET_ | grep _NAME | cut -d = -f 2) ; do 
    ifile=./results/openeo_$1_$(echo $i | rev | cut -d : -f 1 | rev).tif
    [ -f $ifile ] && rm $ifile
    gdal_translate -a_srs $crs -co compress=deflate $i $ifile
  done
}

# $1 file name
# $2 extent
function cut_to_extent(){
  mv $1 $1.tmp.tif
  gdal_translate -a_srs EPSG:32631 -co compress=deflate -b $2 $1.tmp.tif $1
  rm $1.tmp.tif
}


# split netcdf to subsets (bands)
extent=""
for i in $(ls output_*.nc) ; do
  explode_netcdf_bands $i
  if [ "$extent" == "" ] ; then
    extent=$( gdalinfo $ifile | grep "Lower Left\|Upper Right" | sed "s/Lower Left//g;s/Upper Right//g;s/).*//g;s/(//g;s/ //g" | tr '\n' ',' ) #"
    extent="${extent//,/ }"
  fi
done
echo $extent


# split tif-s to bands and cut
for i in $(ls input_*.tif) ; do
  explode_tif_bands $i "$extent"
done
for i in $(ls output_*.tif) ; do
  explode_tif_bands $i "$extent"
done


# split sen2cor jp2-s to bands and cut
for i in $(ls ./sen2cor/L1C/*_B0[2348].jp2) ; do
  explode_jp2_bands $i "$extent"
done
for i in $(ls ./sen2cor/L2A/*_B0[2348]_*.jp2) ; do
  explode_jp2_bands $i "$extent"
done


