import os
import pickle

import numpy as np
import pandas as pd
import geopandas as gpd


def calc_avg_dis(df=None, ds_field=None, mode_field=None,  region_pop_dict=None, from_name_field=None):

    ds_mode_avg_df = df.groupby([ds_field, mode_field, from_name_field])[['dis', 'count']].sum().reset_index(drop=False).rename(
        columns={'dis': '总出行距离',
                 'count': '总出行次数', from_name_field: '出发区域'})
    ds_mode_avg_df['平均出行距离(按次)'] = ds_mode_avg_df['总出行距离'] / ds_mode_avg_df['总出行次数']
    ds_mode_avg_df['平均出行距离(按次)'] = np.around(ds_mode_avg_df['平均出行距离(按次)'], decimals=2)

    ds_avg_df = df.groupby([ds_field, from_name_field])[['dis', 'count']].sum().reset_index(drop=False).rename(
        columns={'dis': '总出行距离',
                 'count': '总出行次数', from_name_field: '出发区域'})
    ds_avg_df['区域人口'] = ds_avg_df['出发区域'].map(region_pop_dict)
    ds_avg_df['平均出行距离(按次)'] = ds_avg_df['总出行距离'] / ds_avg_df['总出行次数']
    ds_avg_df['平均出行距离(按人)'] = ds_avg_df['总出行距离'] / ds_avg_df['区域人口']
    ds_avg_df['平均出行次数(按人)'] = ds_avg_df['总出行次数'] / ds_avg_df['区域人口']

    ds_avg_df['平均出行距离(按次)'] = np.around(ds_avg_df['平均出行距离(按次)'], decimals=2)
    ds_avg_df['平均出行距离(按人)'] = np.around(ds_avg_df['平均出行距离(按人)'], decimals=2)
    ds_avg_df['平均出行次数(按人)'] = np.around(ds_avg_df['平均出行次数(按人)'], decimals=2)


    # 不区分行政区
    all_ds_mode_avg_df = ds_mode_avg_df.groupby([ds_field, mode_field])[['总出行距离', '总出行次数']].sum().reset_index(drop=False)
    all_ds_mode_avg_df['出发区域'] = '佛山'
    all_ds_mode_avg_df['平均出行距离(按次)'] = all_ds_mode_avg_df['总出行距离'] / all_ds_mode_avg_df['总出行次数']
    all_ds_mode_avg_df['平均出行距离(按次)'] = np.around(all_ds_mode_avg_df['平均出行距离(按次)'], decimals=2)

    all_ds_avg_df = ds_mode_avg_df.groupby(ds_field)[['总出行距离', '总出行次数']].sum().reset_index(drop=False)
    all_ds_avg_df['出发区域'] = '佛山'
    all_ds_avg_df['区域人口'] = all_ds_avg_df['出发区域'].map(region_pop_dict)
    all_ds_avg_df['平均出行距离(按次)'] = all_ds_avg_df['总出行距离'] / all_ds_avg_df['总出行次数']
    all_ds_avg_df['平均出行距离(按人)'] = all_ds_avg_df['总出行距离'] / all_ds_avg_df['区域人口']
    all_ds_avg_df['平均出行次数(按人)'] = all_ds_avg_df['总出行次数'] / all_ds_avg_df['区域人口']

    all_ds_avg_df['平均出行距离(按次)'] = np.around(all_ds_avg_df['平均出行距离(按次)'], decimals=2)
    all_ds_avg_df['平均出行距离(按人)'] = np.around(all_ds_avg_df['平均出行距离(按人)'], decimals=2)
    all_ds_avg_df['平均出行次数(按人)'] = np.around(all_ds_avg_df['平均出行次数(按人)'], decimals=2)

    res_ds_avg_df = ds_avg_df._append(all_ds_avg_df)
    res_ds_mode_avg_df = ds_mode_avg_df._append(all_ds_mode_avg_df)

    return res_ds_mode_avg_df, res_ds_avg_df


if __name__ == '__main__':

    # 计算人口
    # 1.计算各区的人口
    data_fldr = r'./data/input/'  # 输入文件路径
    print(r'读取栅格......')

    grid_gdf = gpd.read_file(r'./data/input/grid_wgs.shp')   # 栅格中心点矢量
    print(grid_gdf)

    home_df = pd.read_csv(os.path.join(data_fldr, r'zc_v5_home_detail_count.csv'))  # 居住人口原数据

    # 合并数据表，在home_df中加入栅格中心点数据的行政区信息
    home_df = pd.merge(home_df, grid_gdf[['grid_id', 'name']], left_on='home_grid', right_on='grid_id',
                       how='left')  
    # 去除name为空的(不是居住佛山的)
    home_df.dropna(subset=['name'], axis=0, inplace=True)
    # 聚合统计各区划人口数
    region_pop_df = home_df.groupby(['name'])[['count']].sum().reset_index(drop=False)
    # 总人口数
    all_pop = region_pop_df['count'].sum()
    # 插入一行总人口
    region_pop_df.loc[len(region_pop_df), :] = {'name': '佛山', 'count': all_pop}
    # 转存聚合的人口数据为字典
    region_name_pop_dict = {name: pop for name, pop in zip(region_pop_df['name'],
                                                           region_pop_df['count'])}
    del home_df
    del region_pop_df
    del grid_gdf
    print(region_name_pop_dict)

    # 创建空数据表，存入清洗后的移动数据
    move_df = pd.DataFrame()

    for i in range(1, 9):
        print(i)
        with open(rf'./data/input/clean/fs_move_{i}.df', 'rb') as f:
            _ = pickle.load(f)
            _.drop(columns=['is_local', 'start_half', 'start_hour', 'end_name', 'start_grid_id', 'end_grid_id',
                            'last_grid'],
                   inplace=True, axis=1)
            move_df = move_df._append(_)
    del _  # 为什么不写在循环内？

    move_df.reset_index(inplace=True, drop=True)
    print(len(move_df))

    # 计算按照次数平均的出行距离
    print(move_df['start_name'].unique())
    move_df = move_df[move_df['start_name'].isin(['三水区', '南海区', '高明区', '顺德区', '禅城区'])].copy()  # 切片佛山市区
    move_df = move_df[move_df['dis'] >= 100]    # 切片距离>=100
    move_df['dis'] = move_df['count'] * move_df['dis']   # 距离*人数，总出行距离？？
    region_avg_dis_ds_mode_df, region_avg_dis_ds_df = calc_avg_dis(df=move_df, ds_field='date',
                                                                   mode_field='mode_name',
                                                                   from_name_field='start_name',
                                                                   region_pop_dict=region_name_pop_dict)

    region_avg_dis_ds_mode_df.to_csv(rf'./data/output/区分日期模式.csv',
                                     encoding='utf_8_sig', index=False)
    region_avg_dis_ds_df.to_csv(rf'./data/output/区分日期.csv',
                                encoding='utf_8_sig', index=False)



