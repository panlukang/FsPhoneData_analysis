import pickle
import geopandas as gpd
import pandas as pd

if __name__ == '__main__':
    taz_gdf = gpd.read_file(r'./data/input/fs_taz_wgs.shp')
    taz_gdf.rename(columns={'id': 'taz_id'}, inplace=True)

    district_gdf = gpd.read_file(r'./data/input/佛山面域_WGS.geojson')
    district_gdf.rename(columns={'name': 'district_name'}, inplace=True)

    fs_grid_gdf = gpd.read_file(r'./data/input/fs_grid_wgs.shp')
    print(fs_grid_gdf)

    fs_grid_gdf = gpd.sjoin(fs_grid_gdf, district_gdf[['district_name', 'geometry']])
    fs_grid_gdf.drop(columns=['index_right'], inplace=True, axis=1)
    fs_grid_gdf = gpd.sjoin(fs_grid_gdf, taz_gdf[['taz_id', 'geometry']])

    date_district_df = pd.DataFrame()
    date_taz_df = pd.DataFrame()

    for i in range(1, 9):
        with open(fr'./data/input/clean/fs_move_{i}.df', 'rb') as f:
            df = pickle.load(f)
        print(df.columns)
        print(len(df))

        df = df.groupby(['date', 'start_grid_id', 'end_grid_id'])[['count']].sum().reset_index(drop=False)

        df = pd.merge(df, fs_grid_gdf[['grid_id', 'district_name', 'taz_id']],
                      left_on='start_grid_id', right_on='grid_id')

        df.rename(columns={'district_name': 'home_district_name',
                           'taz_id': 'home_small_zone'}, inplace=True)

        df = pd.merge(df, fs_grid_gdf[['grid_id', 'district_name', 'taz_id']],
                      left_on='end_grid_id', right_on='grid_id')
        df.rename(columns={'district_name': 'work_district_name',
                           'taz_id': 'work_small_zone'}, inplace=True)
        print(len(df))
        _date_taz_df = df.groupby(['date', 'home_small_zone', 'work_small_zone'])[['count']].sum().reset_index(drop=False)
        _date_district_df = df.groupby(['date', 'home_district_name', 'work_district_name'])[['count']].sum().reset_index(drop=False)
        date_taz_df = pd.concat([date_taz_df, _date_taz_df])
        date_district_df = pd.concat([date_district_df, _date_district_df])


    date_district_df = date_district_df.groupby(['date', 'home_district_name',
                                                 'work_district_name'])[['count']].sum().reset_index(drop=False)
    date_taz_df = date_taz_df.groupby(['date', 'home_small_zone',
                                       'work_small_zone'])[['count']].sum().reset_index(drop=False)

    date_taz_df.to_csv(r'./data/output/plot/3.9_出行OD统计(交通小区).csv', encoding='utf_8_sig', index=False)
    date_district_df.to_csv(r'./data/output/plot/3.8_出行OD统计(行政区).csv', encoding='utf_8_sig', index=False)



    # 计算小区的通勤期望线
    # home_work_min_df = pd.read_csv(r'./data/output/佛山手机信令数据/home_work_pop.csv')
    # print(home_work_min_df)
    #
    # # 聚合为小区的层级
    # home_work_min_df = pd.merge(home_work_min_df, fs_grid_gdf[['grid_id', 'taz_id']], left_on='home_grid',
    #                             right_on='grid_id')
    # home_work_min_df.rename(columns={'taz_id': 'home_small_zone'}, inplace=True)
    #
    # home_work_min_df = pd.merge(home_work_min_df, fs_grid_gdf[['grid_id', 'taz_id']], left_on='work_grid',
    #                             right_on='grid_id')
    # home_work_min_df.rename(columns={'taz_id': 'work_small_zone'}, inplace=True)
    #
    # # 聚合为行政区的层级
    # district_hw_df = home_work_min_df.groupby(['home_district_name',
    #                                            'work_district_name'])[['count']].sum().reset_index(inplace=False)
    # district_hw_df.to_csv(r'./data/output/plot/3.6_职住人口关联统计(行政区).csv', encoding='utf_8_sig', index=False)
    #
    # taz_hw_df = home_work_min_df.groupby(['home_small_zone',
    #                                       'work_small_zone'])[['count']].sum().reset_index(inplace=False)
    # taz_hw_df.to_csv(r'./data/output/plot/3.7_职住人口关联统计(小区).csv', encoding='utf_8_sig', index=False)


    # # 计算小区和行政区的人口统计
    # home_grid_df = pd.read_csv(r'./data/output/佛山手机信令数据/home_pop.csv')
    # work_grid_df = pd.read_csv(r'./data/output/佛山手机信令数据/work_pop.csv')
    # home_work_grid_pop = pd.merge(home_grid_df, work_grid_df, left_on='home_grid', right_on='work_grid', how='outer')
    # home_work_grid_pop['home_grid'].fillna(home_work_grid_pop['work_grid'], inplace=True)
    # home_work_grid_pop['district_name_x'].fillna(home_work_grid_pop['district_name_y'], inplace=True)
    # home_work_grid_pop = home_work_grid_pop[['home_grid', 'home_count', 'work_count', 'district_name_x']].copy()
    # home_work_grid_pop.rename(columns={'home_grid': 'grid', 'district_name_x': 'district_name'}, inplace=True)
    # home_work_grid_pop.fillna(0, inplace=True)
    # home_work_grid_pop = pd.merge(home_work_grid_pop, fs_grid_gdf[['grid_id', 'taz_id']], left_on='grid',
    #                               right_on='grid_id')
    # home_work_grid_pop.rename(columns={'taz_id': 'small_zone'}, inplace=True)
    #
    # # 按照小区聚合
    # home_work_taz_pop = home_work_grid_pop.groupby(['small_zone'])[['home_count', 'work_count']].sum().reset_index(
    #     drop=False)
    # home_work_district_pop = home_work_grid_pop.groupby(['district_name'])[
    #     ['home_count', 'work_count']].sum().reset_index(
    #     drop=False)
    # home_work_taz_pop.to_csv(r'./data/output/plot/3.4_人口统计(小区).csv', encoding='utf_8_sig', index=False)
    # home_work_district_pop.to_csv(r'./data/output/plot/3.3_人口统计(区县).csv', encoding='utf_8_sig', index=False)
