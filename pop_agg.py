import os.path
import pickle
import pandas as pd
import geopandas as gpd

if __name__ == '__main__':

    for i in range(1, 9):
        print(i)
        df = pd.read_pickle(r'./data/input/join/move_join_{}'.format(i))
        df_pop_agg = df.groupby(['date', 'start_half',
                                 'start_grid_id', 'start_grid_lon', 'start_grid_lat',
                                 'start_traffic_district_id', 'start_district_name', 'start_city_name',
                                 'end_grid_id', 'end_grid_lon', 'end_grid_lat',
                                 'end_traffic_district_id', 'end_district_name', 'end_city_name',
                                 'last_grid', 'mode'])[['count']].sum().reset_index(drop=False)
        del df
        df_pop_agg.to_csv(r'./data/output/move_pop_{}.csv'.format(i), encoding='utf_8_sig', index=False)
        print('文件{}保存完成'.format(i))
        del df_pop_agg
