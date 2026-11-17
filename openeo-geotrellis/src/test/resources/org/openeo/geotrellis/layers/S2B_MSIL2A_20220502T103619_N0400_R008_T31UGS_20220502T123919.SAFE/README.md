# Sentinel-2 L2A near-nadir test fixture (issue #916)

Contains Copernicus Sentinel data (2022). Terms: https://dataspace.copernicus.eu/terms-and-conditions

Selected files of product `S2B_MSIL2A_20220502T103619_N0400_R008_T31UGS_20220502T123919` (processing baseline
04.00), laid out as in the SAFE product so that `MASK_FILENAME` paths resolve against this directory. The tile is the
eastern neighbour of T31UFS in the same datatake and crosses the swath centre: B05 detectors 4 to 10 are present, and
the lowest B05 viewing zenith on the grid is 0.997°.

| File | Source | Bytes | SHA-256 |
| --- | --- | --- | --- |
| `GRANULE/L2A_T31UGS_A026918_20220502T103700/MTD_TL.xml` | https://sentinel-s2-l2a.s3.amazonaws.com/tiles/31/U/GS/2022/5/2/0/metadata.xml | 626751 | `218f478b754611dfa87d6d136c13b4d63d16e086611dc8b6f870bb3dd6035708` |
| `GRANULE/L2A_T31UGS_A026918_20220502T103700/QI_DATA/MSK_DETFOO_B05.jp2` | https://sentinel-s2-l2a.s3.amazonaws.com/tiles/31/U/GS/2022/5/2/0/qi/DETFOO_B05.jp2 | 27832 | `16ef515da8a890359e6300c0815041eda1bb0102aaaf232edd81edc740d319ef` |

The files are unmodified; the mask is stored under its SAFE name.
