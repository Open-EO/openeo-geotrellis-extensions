package org.openeo.geotrellis.icor;


/*

         template<typename T>
        vector<T> read_array(int fixed = -1) {

            int dim = fixed;
            if (dim == -1) {
                input.read(reinterpret_cast<char*> (&dim), sizeof(int));
            }
            vector<T> arr(dim);
            input.read(reinterpret_cast<char*>(&arr[0]), sizeof(T) * dim);

            return arr;
        }

 
        void read_header() {

            int dim;

            input.read(reinterpret_cast<char*> (&dim), sizeof(int));
            dimensions = dim;
            band = read_array<uint16_t>();
            sza = read_array<double>();
            vza = read_array<double>();
            raa = read_array<double>();
            height = read_array<double>();
            aot = read_array<double>();
            cwv = read_array<double>();
            ozone = read_array<double>();


            bands_size = band.size();
            sza_size = sza.size();
            vza_size = vza.size();
            raa_size = raa.size();
            height_size = height.size();
            aot_size = aot.size();
            cwv_size = cwv.size();
            ozone_size = ozone.size();




        }

        void read_lut() {


            recordsize = 0;
            records = 0;
            input.read(reinterpret_cast<char*> (&recordsize), sizeof(int));
            input.read(reinterpret_cast<char*> (&records), sizeof(int));

            lut_values.resize(records);

            rsut::simple_progress_bar bar;
            rsut::process_timer timer;
            rsut::progress progress(this->records, &bar);

            vector<double> values(recordsize);

            for (size_t i = 0; i < records; i++) {

                values = read_array<double>((int)recordsize);
                lut_values[i] = values;
                ++progress;

            }


[banyait@localhost LUT]$ pwd
/home/banyait/tmp/icor/sentinel2/Templates/LUT
[banyait@localhost LUT]$ ls
S2A_10m.bin  S2A_20m.bin  S2A_60m.bin  S2A_all.bin  S2B_10m.bin  S2B_20m.bin  S2B_60m.bin  S2B_all.bin
[banyait@localhost LUT]$ 

 
*/

public class LookupTableIO {
  
}
