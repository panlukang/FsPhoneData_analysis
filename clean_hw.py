"""计算佛山各区的人口"""
import os
import pandas as pd
import geopandas as gpd


if __name__ == '__main__':

    data_fldr = r'./data/input/'
    # print(home_df['count'])
    # print(home_df['count'].sum())

    # 计算小区的通勤期望线
    # home_work_min_df = pd.read_csv(r'./data/output/佛山手机信令数据/home_work_pop.csv')
    # print(home_work_min_df)
    #
    # # 聚合为行政区的层级
    # district_hw_df = home_work_min_df.groupby(['home_district_name',
    #                                            'work_district_name'])[['count']].sum().reset_index(inplace=False)
    # print(district_hw_df)

    # 交通小区图层
    date_taz_df = pd.DataFrame()
    date_district_df = pd.DataFrame()
    for file in ['move_pop_1.csv', 'move_pop_2.csv', 'move_pop_3.csv',
                 'move_pop_4.csv', 'move_pop_5.csv', 'move_pop_6.csv',
                 'move_pop_7.csv', 'move_pop_8.csv']:

        move_df = pd.read_csv(fr'./data/output/佛山手机信令数据/{file}', nrows=1000)
        move_df = move_df[(move_df['start_district_name'] != '佛山外部') &
                          (move_df['end_district_name'] != '佛山外部')].copy()

        _date_taz_df = move_df.groupby(['date', 'start_traffic_district_id',
                                        'end_traffic_district_id'])[['count']].sum().reset_index(drop=False).rename(
            columns={'start_traffic_district_id': 'home_small_zone',
                     'end_traffic_district_id': 'work_small_zone'})

        _date_district_df = move_df.groupby(['date', 'start_district_name',
                                             'end_district_name'])[['count']].sum().reset_index(drop=False).rename(
            columns={'start_district_name': 'home_district_name',
                     'end_district_name': 'work_district_name',})

        date_taz_df = pd.concat([date_taz_df, _date_taz_df])

        date_district_df = pd.concat([date_district_df, _date_district_df])

    date_district_df = date_district_df.groupby(['home_district_name',
                                                 'work_district_name'])[['count']].sum().reset_index(drop=False)

    date_taz_df = date_taz_df.groupby(['home_small_zone',
                                       'work_small_zone'])[['count']].sum().reset_index(drop=False)

    print(date_district_df)
    print(date_taz_df)
