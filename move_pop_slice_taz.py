import os.path
import pickle
import pandas as pd
import geopandas as gpd


def filter_od_df(start_grid_field=None,
                 end_grid_field=None,
                 target_region_df=None,
                 region_grid_id_field=None,
                 _type='any',
                 od_df=None):
    '''
    字段匹配，筛选佛山内外，匹配佛山内行政区划
    '''
    # 转换int64、float64字段为int32、float32，节省内存占用
    od_df[od_df.select_dtypes('int64').columns] = od_df.select_dtypes('int64').astype('int32')
    od_df[od_df.select_dtypes('float64').columns] = od_df.select_dtypes('float64').astype('float32')
    target_region_df[target_region_df.select_dtypes('int64').columns] = target_region_df.select_dtypes('int64').astype(
        'int32')
    target_region_df[target_region_df.select_dtypes('float64').columns] = target_region_df.select_dtypes(
        'float64').astype('float32')

    # 匹配起点grid_id的经纬度
    filter_od_df = pd.merge(od_df, target_region_df[[region_grid_id_field, 'name', 'lon', 'lat', 'geometry']],
                            left_on=[start_grid_field], right_on=[region_grid_id_field],
                            how='left')

    del od_df

    # 起点na值填充'佛山外部'
    filter_od_df['name'] = filter_od_df['name'].fillna('佛山外部')

    # 删去无用字段，重命名
    filter_od_df.drop(columns=[region_grid_id_field], axis=1, inplace=True)
    filter_od_df.rename(columns={'name': 'start_district_name',
                                 'lon': 'start_grid_lon',
                                 'lat': 'start_grid_lat',
                                 'geometry': 'start_geometry'}, inplace=True)

    # 匹配终点grid_id的经纬度
    filter_od_df = pd.merge(filter_od_df, target_region_df[[region_grid_id_field, 'name', 'lon', 'lat', 'geometry']],
                            left_on=[end_grid_field], right_on=[region_grid_id_field],
                            how='left')

    # 终点na值填充'佛山外部'
    filter_od_df['name'] = filter_od_df['name'].fillna('佛山外部')

    # 删去无用字段，重命名
    filter_od_df.drop(columns=[region_grid_id_field], axis=1, inplace=True)
    filter_od_df.rename(columns={'name': 'end_district_name',
                                 'lon': 'end_grid_lon',
                                 'lat': 'end_grid_lat',
                                 'geometry': 'end_geometry'}, inplace=True)

    filter_od_df.reset_index(inplace=True, drop=True)

    return filter_od_df


def df_spatial_join(geofile=None,
                    od_df=None,
                    od_df_geometry=None,
                    join_column_name=None,
                    output_column_name=None):
    """
    出行数据空间匹配区划数据
    :param geofile: 区划数据
    :param od_df: 出行数据
    :param od_df_geometry: 输入df的geometry字段
    :param join_column_name: 匹配区划字段名
    :param output_column_name: 输出区划字段名
    """

    # 转换int64、float64字段为int32、float32，节省内存占用
    od_df[od_df.select_dtypes('int64').columns] = od_df.select_dtypes('int64').astype('int32')
    od_df[od_df.select_dtypes('float64').columns] = od_df.select_dtypes('float64').astype('float32')
    geofile[geofile.select_dtypes('int64').columns] = geofile.select_dtypes('int64').astype('int32')
    geofile[geofile.select_dtypes('float64').columns] = geofile.select_dtypes('float64').astype('float32')

    # 清洗经纬度为空的数据
    od_df.dropna(subset=['start_grid_lon', 'start_grid_lat', 'end_grid_lon', 'end_grid_lat'],
                 how='any', inplace=True)

    # 空间关联，找出每个居住地位于哪个行政区划中。
    od_gdf = gpd.GeoDataFrame(
        od_df, geometry=od_df_geometry, crs='EPSG:4326')
    od_gdf = gpd.sjoin(od_gdf, geofile, how="left", predicate='within')

    del od_df, geofile

    # 去除无用列，重命名
    od_gdf.drop(columns=['index_right'], inplace=True)
    od_gdf.rename(columns={join_column_name: output_column_name}, inplace=True)

    # 清洗空间匹配字段为空的数据
    od_gdf.dropna(subset=[output_column_name], how='any', inplace=True)

    # id为int类型，在sjoin中会变为float，判断并改回原数据类型
    if od_gdf[output_column_name].dtypes == 'float64':
        od_gdf[output_column_name] = od_gdf[output_column_name].astype('int32')
    return od_gdf


if __name__ == '__main__':

    print(r'读取栅格......')
    # 读取广东栅格中心点，带交通、行政区划坐标
    with open(r'./data/input/gz_grid_join_wgs', 'rb') as f:
        grid_gdf = pickle.load(f)
    print(r'读取栅格完成......')

    data_fldr = r'./data/input/'

    # 1.清洗出行数据
    movement_inner_file_list = []  # 建立空列表存储文件名，便于后续遍历清洗
    movement_in_out_file_list = []
    movement_out_in_file_list = []
    for file in os.listdir(data_fldr):
        if 'zcsj_fs3_move_month_inner' in file:
            movement_inner_file_list.append(file)
        if 'zcsj_fs3_move_month_in_to_out' in file:
            movement_in_out_file_list.append(file)
        if 'zcsj_fs3_move_month_out_to_in' in file:
            movement_out_in_file_list.append(file)

    print(movement_out_in_file_list)
    print(movement_inner_file_list)
    print(movement_in_out_file_list)

    N = 10  # 切片数
    i = 1

    for file in movement_inner_file_list + movement_out_in_file_list + movement_in_out_file_list:
        print(file)
        i += 1
        df = pd.read_csv(os.path.join(data_fldr, file))
        # 判断是否有'last_grid'字段，没有则添加字段并设置为-1，便于后续统一处理
        if 'last_grid' in list(df.columns):
            pass
        else:
            df['last_grid'] = -1

        results = []

        # 切片处理
        id_list = [i for i in range(1, len(df) + 1)]
        df['id_list'] = id_list
        df['label'] = list(pd.cut(df['id_list'], bins=N, labels=[i for i in range(1, N + 1)]))

        for _label in [i for i in range(1, N + 1)]:
            slice_df = df[df['label'] == _label].copy()
            print('文件{0}切片{1}处理中'.format(i, _label))

            # 字段连接，匹配OD数据与区划矢量数据
            result = pd.merge(slice_df, grid_gdf, 
                              left_on='start_grid_id', right_on='grid_id',
                              how='inner')
            del slice_df
            result['start_grid_lon'] = result['geometry'].apply(lambda x: x.x)
            result['start_grid_lat'] = result['geometry'].apply(lambda x: x.y)
            result.drop(columns=['grid_id', 'lon', 'lat','geometry'], inplace=True)
            result.rename(columns={'taz_id': 'start_small_zone', 
                                   'city_name':'start_city_name', 
                                   'district_name':'start_district_name'}, inplace=True)

            result = pd.merge(result, grid_gdf, 
                              left_on='end_grid_id', right_on='grid_id',
                              how='inner')
            result['end_grid_lon'] = result['geometry'].apply(lambda x: x.x)
            result['end_grid_lat'] = result['geometry'].apply(lambda x: x.y)
            result.drop(columns=['grid_id', 'lon', 'lat','geometry'], inplace=True)
            result.rename(columns={'taz_id': 'end_small_zone', 
                                   'city_name':'end_city_name', 
                                   'district_name':'end_district_name'}, inplace=True)

            results.append(result)

        res_df = pd.DataFrame()
        for _result in results:
            res_df = res_df._append(_result)
        res_df.reset_index(inplace=True, drop=True)

        with open(rf'./data/input/join/move_join_fix{i}', 'wb') as f:
            pickle.dump(res_df, f)
        print('文件{}完成'.format(i))
