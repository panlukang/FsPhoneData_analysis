import os
import pickle

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

if __name__ == '__main__':
    '''
    输入居住-工作地人口数据, 广东栅格中心点矢量数据
    空间关联两表得到区划信息
    清洗数据
    基于居住-工作地栅格点id, 聚合人口数
    '''
    data_fldr = r'./data/input/'  # 输入文件路径

    # 居住-工作地人口数据
    print(r'读取居住-工作地人口数据......')
    home_work_df = pd.read_csv(
        r'./data/input/zc_v5_homework.csv', low_memory=False)
    print(home_work_df)

    # 广东栅格中心点数据
    print(r'读取栅格......')
    # grid_gdf = gpd.read_file(r'./data/input/grid_wgs.shp')  # 栅格中心点矢量
    with open(r'./data/input/gz_grid_wgs', 'rb') as f:
        grid_gdf = pickle.load(f)
    print(grid_gdf)

    # 佛山行政区划shp
    fs = gpd.read_file(r'./data/input/佛山面域_WGS.geojson')

    # 清洗经纬度为空的数据
    home_work_df.dropna(subset=['home_lon', 'home_lat', 'work_lon', 'work_lat'], how='any', inplace=True)

    # 根据经纬度生成Point
    home_work_df['home_point'] = home_work_df.loc[:, ['home_lon', 'home_lat']].apply(lambda x: Point((x[0], x[1])), axis=1)
    home_work_df['work_point'] = home_work_df.loc[:, ['work_lon', 'work_lat']].apply(lambda x: Point((x[0], x[1])), axis=1)

    # 空间关联，找出每个居住地位于哪个行政区划中。
    home_work_gdf = gpd.GeoDataFrame(home_work_df, geometry='home_point', crs='EPSG:4326')
    home_work_gdf = gpd.sjoin(home_work_gdf, fs, how="left", predicate='within')

    # 去除无用列，重命名
    home_work_gdf.drop(columns=['index_right', 'level', 'len', 'home_city', 'work_city'], inplace=True)
    home_work_gdf.rename(columns={'name': 'home_district_name'}, inplace=True)

    # 空间关联，找出每个工作地位于哪个行政区划中。
    home_work_gdf.set_geometry('work_point', inplace=True, crs='EPSG:4326')
    home_work_gdf = gpd.sjoin(home_work_gdf, fs, how="left", predicate='within')

    # 去除无用列，重命名
    home_work_gdf.drop(columns=['index_right', 'level', 'len'], inplace=True)
    home_work_gdf.rename(columns={'name': 'work_district_name'}, inplace=True)

    # # 匹配home_grid的行政区信息
    # home_work_df = pd.merge(home_work_df, grid_gdf[['grid_id', 'name']],
    #                     left_on='home_grid', right_on='grid_id',
    #                     how='left')

    # home_work_df.drop(columns=['grid_id'], axis=1, inplace=True)
    # home_work_df.rename(columns={'name': 'home_district_name'}, inplace=True)

    # # 匹配work_grid的行政区信息
    # home_work_df = pd.merge(home_work_df, grid_gdf[['grid_id', 'name']],
    #                     left_on='work_grid', right_on='grid_id',
    #                     how='left')

    # home_work_df.drop(columns=['grid_id'], axis=1, inplace=True)
    # home_work_df.rename(columns={'name': 'work_district_name'}, inplace=True)

    # 聚合统计各栅格点居住人口数
    ras_sum_pop_df = home_work_gdf.groupby(['home_grid', 'home_lon', 'home_lat', 'home_district_name',
                                            'work_grid', 'work_lon', 'work_lat', 'work_district_name'])[['count']].sum().reset_index()

    # 将对象保存csv文件
    ras_sum_pop_df.to_csv(r'./data/output/home_work_pop.csv', index=False)
    # ras_sum_pop_df.to_csv(r'./data/output/home_work_pop_utf8.csv', encoding='utf-8', index=False)

    print('完成')
