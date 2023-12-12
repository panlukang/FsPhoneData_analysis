# 佛山手机信令可视化数据处理

ana.py : 聚合居住人口数据，基于清洗后出行数据计算出行距离

clean_move.py : 清洗出行数据

home_pop.py : 居住地人口数据处理

work_pop.py : 工作地人口数据处理

home_work_pop.py : 居住地、工作地人口数据处理

move_pop_slice.py : 基于行政区划、交通区划矢量，空间匹配出行数据加入起终点信息（切片处理）

move_pop_slice_taz.py : 基于带行政区划、交通区划信息的栅格中心点，表连接匹配出行数据加入起终点信息（切片处理）

pop_agg.py : move_pop_slice.py/move_pop_slice_taz.py处理结果数据，聚合起终点信息相同的人口信息
