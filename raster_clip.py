import os

raster_data = '/mnt/e/test/elv_mosaic_AM_resample_bilinear.tif'

polygon_shp = '/home/teste.shp'

out_path = '/mnt/j/AM/MNT_CLIP/'

unique_id = 'COD_BACIA'

core_count = 7

def get_fids(polygon_shp,unique_id):
    ds = ogr.Open(polygon_shp)
    layer = ds.GetLayer()
    jobs = []
    for polygon in layer:
        pol_id = polygon.GetField(unique_id)
        jobs.append(pol_id)

    ds = None
    return jobs

def clip_raster(mosaick,polygon_shp,pol_id,unique_id,out_folder):
    '''
    This function clips the vrt mosaick creating another vrt in memory
    reprojected to projection african albers equal area for statistical histogram
    processing and area calculations
    '''
    warp_name = 'clip_{}.tif'.format(pol_id)
    warp_path = os.path.join(out_folder,warp_name)
    memory_cache = 1.8 #in Gb cant be greater than 2Gb
    mem_cache = memory_cache*10**9
    gdal.SetCacheMax(mem_cache)

    memory = 1.8 #in Gb cant be greater than 2Gb
    mem = memory*10**9
    wrp_options = gdal.WarpOptions( multithread=True,
                                    warpMemoryLimit=mem,
                                    srcNodata=-1,
                                    dstNodata=-1,
                                    outputType=gdal.GDT_Int16,
                                    workingType=gdal.GDT_Int16,
                                    cutlineDSName=polygon_shp,
                                    cutlineWhere="{} = {}".format(unique_id,pol_id),
                                    cropToCutline=True,
                                    creationOptions=["BLOCKXSIZE=256",
                                                     "BLOCKYSIZE=256",
                                                     "NUM_THREADS=ALL_CPUS",
                                                     "tiled=yes",
                                                     "compress=lzw",
                                                     "bigtiff=yes"])
    if not os.path.isfile(warp_path):
        ds = gdal.Warp(warp_path, mosaick,options=wrp_options)
        ds = None
    print(pol_id)
    return warp_path

def parallel_clip(jobs_,mosaick,polygon_shp,unique_id,out_folder,core_count):
    pool = Pool(processes=core_count, maxtasksperchild=10)
    jobs = {}
    for pol_id in jobs_:
        jobs[pol_id = pool.apply_async(clip_raster,[mosaick,polygon_shp,pol_id,unique_id,out_folder])
    results = {}
    for pol_id,result in jobs.items():
        try:
            results[pol_id] = result.get()
        except (KeyboardInterrupt, SystemExit):
            print('Caught ctrlkey + C event, terminating...')
            pool.terminate()
            pool.join()
            raise
        except Exception as e:
            print(e)
        else:
            pool.close()
            pool.join()

def main():
    '''
    main function where all the process starts
    '''
    jobs = get_fids(polygon_shp,unique_id)

    parallel_clip(jobs,mosaick,polygon_shp,unique_id,out_path,core_count)
    print('finished!')

if __name__ == "__main__":
    main()