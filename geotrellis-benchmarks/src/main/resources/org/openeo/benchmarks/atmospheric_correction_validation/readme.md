# Validation steps manual

This manual covers the basics on how to generate validation reference data using ICOR. 
This manual is for Sentinel2 for now, but Landsat-8 may be added in the future. 

Note that openeo-geotrellis-extensions implements the ICOR correction, not sen2cor. 
ICOR and Sen2cor produces different results (2-5% deviation in absolute values in the 0..1 reflectance range), if you are happy with that confidance level, then just

 * run the atmospheric_correction process
 * download a Sen2cor processed L2A image from your favorite layer
 * do the comparison locally  

## Collect inputs

Collect the necessary input files into ./tmp folder (respective of the bash scripts). 
These files have to be matching in size with the Sentinel tiling.
You can either generate them using Biopar (VITO internal software) or download them using OpenEO (but be sure to use the correct crs for the image collection, such as EPSG:62631 for tile 31UFS).
The spatial resolution does not matter because it will be resampled automatically.
The idea is to bypass Biopar and run ICOR's standalone ac_runner directly.

You will need the following non-image data:

 * response curve for the mission (S2A/S2B) the image was taken with
 * lookup tables for all bands & bands at 10m resolution, for the perspective S2A/S2B
 
and the following images:

 * sza,vza,raa angles (in degrees)
 * all reflectances
 * scaled radiance for all bands and bands at 10m resolution; note that Sentinel2 L1C is earth-sun corrected reflectance -> if you know the angles and the band irradiances (these are all in the L1C product metadata), you can compute it from reflectance
 * elevation in km (NOAA DEM for example?)
 * water and cloud masks (from scene classification?)
 * aerosol optical thickness (for example CAMS's aod550)

## Run OpenEO

Edit validate_atmocorr.py at your convenience and run the scripts.
The parameters in the file produces a 15x11km area, where both clouds, water, forest, city, industrial area,... is observable.

## Run ICOR

Compile ac_runner and put on $PATH:
export PATH=path_to_ac_runner:$PATH

Make sure gdal is installed.

Run them from the working directory 

01_prepare_icor.sh
02_run.sh
03_postproc.sh

The results will be saved into ./results folder. Note that all bands will split to separate images and ICOR result will be cut to the extent of the OpenEO result.

