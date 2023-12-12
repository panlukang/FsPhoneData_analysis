import os
import pickle

import numpy as np
import pandas as pd
import geopandas as gpd

if __name__ == '__main__':
    '''
    输入工作地人口数据, 广东栅格中心点矢量数据
    融合两表得到区划信息
    清洗数据
    基于栅格点id, 聚合人口数
    '''
    data_fldr = r'./data/input/'  # 输入文件路径

    # 工作地人口数据
    print(r'读取工作地人口数据......')
    work_df = pd.read_csv(
        r'./data/input/zc_v5_work_detail_count.csv')
    print(work_df)

    # 广东栅格中心点数据
    print(r'读取栅格......')
    # grid_gdf = gpd.read_file(r'./data/input/grid_wgs.shp')  # 栅格中心点矢量
    with open(r'./data/input/gz_grid_wgs', 'rb') as f:
        grid_gdf = pickle.load(f)
    print(grid_gdf)

    # 合并数据表，在work_df中加入栅格中心点数据的行政区信息
    work_df = pd.merge(work_df, grid_gdf[['grid_id', 'name']],
                       left_on='work_grid', right_on='grid_id',
                       how='left')

    # 清洗数据，去除name为空的(不是佛山工作的)
    work_df.dropna(subset=['name'], axis=0, inplace=True)

    # 聚合统计各栅格点工作人口数
    ras_sum_pop_df = work_df.groupby(['work_grid', 'work_lon', 'work_lat', 'name'])[['count']].sum().reset_index(
        drop=False)

    # 重命名列
    ras_sum_pop_df.rename(columns={'name': 'district_name', 'count': 'work_count'}, inplace=True)

    # 将对象保存csv文件
    ras_sum_pop_df.to_csv(r'./data/output/work_pop.csv', index=False)
    # ras_sum_pop_df.to_csv(r'./data/output/work_pop_utf8.csv', encoding='utf-8', index=False)

    print('完成')