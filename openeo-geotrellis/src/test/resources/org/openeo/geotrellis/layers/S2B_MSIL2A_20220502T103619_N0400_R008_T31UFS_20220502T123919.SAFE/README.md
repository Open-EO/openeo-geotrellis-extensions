# Sentinel-2 L2A test fixture (issue #916)

Contains Copernicus Sentinel data (2022). Terms: https://dataspace.copernicus.eu/terms-and-conditions

Selected files of product `S2B_MSIL2A_20220502T103619_N0400_R008_T31UFS_20220502T123919` (processing baseline
04.00), laid out as in the SAFE product so that `MASK_FILENAME` paths resolve against this directory.

| File | Source | Bytes | SHA-256 |
| --- | --- | --- | --- |
| `GRANULE/L2A_T31UFS_A026918_20220502T103700/MTD_TL.xml` | https://sentinel-s2-l2a.s3.amazonaws.com/tiles/31/U/FS/2022/5/2/0/metadata.xml | 547797 | `602ed672ec3f1fa9dcf794d9a17c5edfc8c7f3449e4ecfe2b79363303cd34935` |
| `GRANULE/L2A_T31UFS_A026918_20220502T103700/QI_DATA/MSK_DETFOO_B05.jp2` | https://sentinel-s2-l2a.s3.amazonaws.com/tiles/31/U/FS/2022/5/2/0/qi/DETFOO_B05.jp2 | 26632 | `13d0c893656eccd89579826c03a54325bbd1ffc3af7e4797ec05272bb29939ec` |

The files are unmodified; the mask is stored under its SAFE name. Its native labels are 0 to 6, but GDAL's
reduced-resolution levels 1/4, 1/8 and 1/16 contain a label 7, which tests use to show those levels are never read.
