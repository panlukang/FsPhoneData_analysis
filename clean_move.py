import os.path
import pickle
import time

import pandas as pd
import geopandas as gpd
from geopy.distance import distance
from shapely.geometry import Point
from shapely import unary_union
import multiprocessing


mode_name_map = {0: '其他', 1: '公路', 2: '铁路', 3: '飞机', 4: '地铁', 5: '高铁'}
local_map = {'Y': '常住', '其他': '流动'}


def filter_od_df(start_grid_field=None, end_grid_field=None,
                 target_region_df=None,
                 region_grid_id_field=None,
                 _type='any', od_df=None):
    # start 和 end 有一个在target_grid_id_list内
    filter_od_df = pd.merge(od_df, target_region_df[[region_grid_id_field, 'name', 'lon', 'lat']],
                            left_on=[start_grid_field], right_on=[region_grid_id_field],
                            how='left')
    filter_od_df['name'] = filter_od_df['name'].fillna('佛山外部')
    filter_od_df.drop(columns=[region_grid_id_field], axis=1, inplace=True)
    filter_od_df.rename(columns={'name': 'start_name',
                                 'lon': 'start_lng', 'lat': 'start_lat'}, inplace=True)

    filter_od_df = pd.merge(filter_od_df, target_region_df[[region_grid_id_field, 'name', 'lon', 'lat']],
                            left_on=[end_grid_field], right_on=[region_grid_id_field],
                            how='left')
    filter_od_df['name'] = filter_od_df['name'].fillna('佛山外部')
    filter_od_df.drop(columns=[region_grid_id_field], axis=1, inplace=True)
    filter_od_df.rename(columns={'name': 'end_name', 'lon': 'end_lng', 'lat': 'end_lat'}, inplace=True)

    filter_od_df.dropna(subset=['start_name', 'end_name'], how='all', inplace=True)
    filter_od_df.reset_index(inplace=True, drop=True)

    return filter_od_df


def calc_dis(df=None, start_x=None, start_y=None,
             end_x=None, end_y=None, process_name=None):
    print(rf'{process_name}一共{len(df)}条数据...')
    na_index = df[[start_x, start_y, end_x, end_y]].isnull().T.any()
    na_df = df[na_index].copy()

    df.drop(index=df[na_index].index, axis=0, inplace=True)
    df['dis'] = df[[start_x, start_y, end_x, end_y]].apply(
        lambda item: distance((item[1], item[0]), (item[3], item[2])).m,
        axis=1)
    na_df['dis'] = -1
    df = df._append(na_df)
    df.reset_index(inplace=True, drop=True)
    print(len(df[df['dis'] < 0]))
    print(na_df[['start_grid_id', 'end_grid_id']])
    df['mode'] = df['mode'].fillna(0)
    df['mode'] = df['mode'].astype(int)
    df['mode_name'] = df['mode'].map(mode_name_map)
    df['mode_name'] = df['mode_name'].fillna('其他')

    df['start_hour'] = df['start_half'].apply(lambda x: int(x.split('-')[0]))

    df['is_local'] = df['is_local'].map(local_map)
    df['is_local'] = df['is_local'].fillna('流动')

    return df


def generate_region_grid():
    fs_wgs_region_gdf = gpd.read_file(r'./data/input/佛山面域_WGS.geojson')

    # geohash对应表
    loc_df = pd.read_csv(os.path.join(r'./data/input/', r'zcsj_fs3_grid_dic.csv'))
    loc_df.dropna(subset=['lon', 'lat'], how='any', inplace=True)
    loc_df.drop_duplicates(subset='grid_id', inplace=True, keep='first')
    loc_df['geometry'] = loc_df[['lon', 'lat']].apply(lambda x: Point((x[0], x[1])), axis=1)
    loc_gdf = gpd.GeoDataFrame(loc_df, geometry='geometry', crs='EPSG:4326')
    loc_gdf_with_name = gpd.sjoin(loc_gdf, fs_wgs_region_gdf[['name', 'geometry']], how='left')
    loc_gdf_with_name.drop(columns=['index_right'], axis=1, inplace=True)
    loc_gdf_with_name.reset_index(inplace=True, drop=True)
    loc_gdf_with_name.to_file(r'./data/input/grid_wgs.shp', encoding='gbk')


def clean_move_df(move_df=None, flag=None, loc_gdf=None):

    print(move_df['date'].unique())
    print(move_df['mode'].unique())
    print(move_df['start_half'].unique())
    print(move_df['is_local'].unique())
    print(len(move_df['is_local']))

    # 统计佛山相关的出行
    filter_move_df = filter_od_df(start_grid_field='start_grid_id', end_grid_field='end_grid_id',
                                  region_grid_id_field='grid_id',
                                  target_region_df=loc_gdf,
                                  od_df=move_df)

    print(filter_move_df)
    print(filter_move_df.columns)

    # 计算每个出行的直线距离, 按照日期来切
    filter_move_df.reset_index(inplace=True, drop=True)
    N = 10
    pool = multiprocessing.Pool(processes=N)
    results = []

    id_list = [i for i in range(1, len(filter_move_df) + 1)]
    filter_move_df['id'] = id_list
    filter_move_df['label'] = list(pd.cut(filter_move_df['id'], bins=N,
                                          labels=[i for i in range(1, N + 1)]))

    for _label in [i for i in range(1, N + 1)]:
        slice_df = filter_move_df[filter_move_df['label'] == _label]
        result = pool.apply_async(calc_dis, args=(slice_df,
                                                  'start_lng', 'start_lat', 'end_lng', 'end_lat', rf'{_label}进程'))
        results.append(result)
    pool.close()
    pool.join()

    res_df = pd.DataFrame()
    for _result in results:
        res_df = res_df._append(_result.get())
    res_df.reset_index(inplace=True, drop=True)
    # 去除直线出行距离小于500米的行
    # res_df.drop(index=res_df[res_df['dis'] <= 500].index, inplace=True, axis=0)
    print(len(res_df))
    with open(rf'./data/input/clean/fs_move_{flag}.df', 'wb') as f:
        pickle.dump(res_df[['date', 'is_local', 'start_half', 'start_hour',
                            'start_name', 'end_name', 'start_grid_id', 'end_grid_id', 'last_grid', 'count', 'dis', 'mode_name']], f)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    print(r'读取栅格......')
    data_fldr = r'./data/input/'
    grid_gdf = gpd.read_file(r'./data/input/grid_wgs.shp')
    grid_gdf['lon'] = grid_gdf['geometry'].apply(lambda x: x.x)
    grid_gdf['lat'] = grid_gdf['geometry'].apply(lambda x: x.y)
    print(grid_gdf)

    # 1.清洗出行数据
    movement_inner_file_list = []
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

    i = 0
    for file in movement_inner_file_list + movement_out_in_file_list + movement_in_out_file_list:
        print(file)
        i += 1
        df = pd.read_csv(os.path.join(data_fldr, file))
        if 'last_grid' in list(df.columns):
            pass
        else:
            df['last_grid'] = -1
        clean_move_df(move_df=df, loc_gdf=grid_gdf, flag=i)



    # # work
    # work_df = pd.read_csv(os.path.join(data_fldr, r'zc_v5_work_detail_count.csv'))
    # # home_work
    # home_work_df = pd.read_csv(os.path.join(data_fldr, r'zc_v5_homework.csv'))


    # loc_gdf.to_file(r'./data/input/grid.shp', encoding='gbk')





