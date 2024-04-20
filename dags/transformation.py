import json
import os
import traceback
import warnings
from datetime import datetime, date as datetime_date

import numpy as np
from matplotlib import pyplot as plt
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder

# from src.JdVVNL_Load_Forcastion import logger
# from src.JdVVNL_Load_Forcastion.entity.config_entity import DataTransformationConfig

warnings.filterwarnings("ignore")
import pandas as pd
import seaborn as sns

color_pal = sns.color_palette()
plt.style.use('fivethirtyeight')
# from src.JdVVNL_Load_Forcastion.utils.common import add_lags, create_features, data_from_weather_api, \
#     holidays_list, uom, sensor_data
# from sklearn.cluster import DBSCAN
#
#
# class DataTransformation:
#     def __init__(self):
#         self.config = config
#         self.le = LabelEncoder()

def initiate_data_transformation(self):
    try:
        data_files = [file for file in os.listdir(self.config.data_file_path) if file.startswith("circle_id")]
        startDate = '2024-01-01 00:00:00'
        endDate = '2024-03-30 23:59:59'

        startDate = datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
        endDate = datetime.strptime(endDate, '%Y-%m-%d %H:%M:%S')
        holiday_lst = holidays_list(startDate, endDate)
        # print(holiday_lst)
        for data_file in data_files:
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.max_rows', 5000)
            df1 = pd.read_parquet(os.path.join(self.config.data_file_path, data_file))
            complete_df = df1[
                ['creation_time', 'sensor_id', 'instant_cum_KW', 'instant_cum_KVA', 'R_Voltage', 'Y_Voltage',
                 'B_Voltage', 'R_Current', 'Y_Current', 'B_Current', 'R_PF', 'Y_PF', 'B_PF', 'cumm_PF', 'status',
                 'opening_KWh', 'closing_KWh', 'opening_KVAh', 'closing_KVAh', 'count']]
            complete_df['creation_time'] = pd.to_datetime(complete_df['creation_time'])

            sen_list = list(complete_df['sensor_id'].unique())
            s_data = sensor_data(sen_list)
            # print(len(s_data['site_id'].unique()))
            dframe = s_data.merge(complete_df, on='sensor_id', how="inner")
            dframe['is_holiday'] = dframe['creation_time'].dt.date.isin(holiday_lst).astype(int)
            dframe.set_index('creation_time', inplace=True)
            dframe['label_sensor'] = self.le.fit_transform(dframe['sensor_id'])
            sensor_ids = dframe['label_sensor'].unique()
            self.sensorDecode(sensor_ids)
            outlier_dict = {}
            for s_id, data in dframe.groupby('label_sensor'):
                s_df = data.copy()

                """ current, voltage, PF, kW, kVA checks"""

                """ PF """
                if len(s_df[s_df['cumm_PF'] > 1]) > 1:
                    s_df.loc[s_df['cumm_PF'] > 1, 'opening_KWh'] = np.nan

                no_current_df = s_df[(s_df['R_Current'] == 0) & (s_df['B_Current'] == 0) & (s_df['Y_Current'] == 0)]
                load_with_no_current_df = no_current_df[
                    (no_current_df['instant_cum_KVA'] > 0.03) & (no_current_df['instant_cum_KW'] > 0.03)]
                if len(load_with_no_current_df) != 0:
                    if len(no_current_df[(no_current_df['instant_cum_KW'] > 0) | (
                            no_current_df['instant_cum_KVA'] > 0)]) < len(s_df) / 100:
                        s_df.loc[load_with_no_current_df.index, 'opening_KWh'] = np.nan
                    else:
                        continue

                no_voltage_df = s_df[(s_df['R_Voltage'] == 0) & (s_df['Y_Voltage'] == 0) & (s_df['B_Voltage'] == 0)]
                no_voltage_but_cuurrent = no_voltage_df[
                    (no_voltage_df['R_Current'] != 0) & (no_voltage_df['B_Current'] != 0) & (
                            no_voltage_df['Y_Current'] != 0)]
                if len(no_voltage_but_cuurrent) != 0:
                    if len(no_voltage_df[(no_voltage_df['R_Current'] != 0) & (no_voltage_df['B_Current'] != 0) & (
                            no_voltage_df['Y_Current'] != 0)]) < len(s_df) / 100:
                        s_df.loc[no_voltage_but_cuurrent.index, 'opening_KWh'] = np.nan

                filtered_df = s_df[s_df['opening_KWh'] > s_df['closing_KWh']]
                if len(filtered_df) != 0:
                    if len(filtered_df) < len(s_df) / 100:
                        s_df.loc[filtered_df.index, 'opening_KWh'] = np.nan
                    else:
                        continue

                """ done"""

                if len(s_df) > 3000:
                    description = s_df.describe()
                    Q2 = description.loc['50%', 'opening_KWh']
                    if Q2 < 1:
                        continue

                    # outage situation
                    s_df.loc[s_df['opening_KWh'] == 0, "opening_KWh"] = np.nan
                    s_df.loc[s_df['opening_KWh'].first_valid_index():]
                    s_df.bfill(inplace=True)

                    # missing packet
                    sensor_df = s_df.resample(rule="15min").asfreq()
                    sensor_df.interpolate(method="linear", inplace=True)

                    # no consumption / same reading
                    if sensor_df['opening_KWh'].nunique() < 10:
                        continue

                    # previous value of opening_KWh
                    sensor_df['prev_KWh'] = sensor_df['opening_KWh'].shift(1)
                    sensor_df.dropna(inplace=True)
                    if len(sensor_df[sensor_df['prev_KWh'] > sensor_df['opening_KWh']]) > 25:
                        continue

                    # consumed unit
                    sensor_df['consumed_unit'] = sensor_df['opening_KWh'] - sensor_df['prev_KWh']
                    sensor_df.loc[sensor_df['consumed_unit'] < 0, "opening_KWh"] = sensor_df["prev_KWh"]
                    sensor_df.loc[sensor_df['consumed_unit'] < 0, "consumed_unit"] = 0

                    if sensor_df['consumed_unit'].nunique() < 10:
                        continue

                    # eliminating id's based on slope
                    numeric_index = pd.to_numeric(sensor_df.index)
                    correlation = np.corrcoef(numeric_index, sensor_df['opening_KWh'])[0, 1]
                    coeffs = np.polyfit(numeric_index, sensor_df['opening_KWh'], 1)

                    slope = coeffs[0]
                    if not np.abs(correlation) > 0.8 and slope > 0:
                        continue

                    # outlier detection
                    epsilon = 11
                    min_samples = 3
                    dbscan = DBSCAN(eps=epsilon, min_samples=min_samples)
                    # outlier_labels = dbscan.fit_predict(s_df[['consumed_unit']])
                    # s_df['outlier'] = outlier_labels
                    sensor_df['db_outlier'] = dbscan.fit_predict(sensor_df[['consumed_unit']])

                    # print('no. of outliers were:',len(s_df[s_df['db_outlier']==-1]))
                    sensor_df.loc[sensor_df['db_outlier'] == -1, 'consumed_unit'] = np.nan
                    #  s_df.loc[outlier_dict[indices[0]].index,'consumed_unit'] = np.nan
                    sensor_df.fillna(method="bfill", inplace=True)
                    # description1 = sensor_df.describe()
                    # Q1 = description1.loc['25%', 'consumed_unit']
                    # Q3 = description1.loc['75%', 'consumed_unit']
                    # # IQR
                    # u_limit = Q3 + ((Q3 - Q1) * 2)
                    # # Z_test
                    # z_scores = ((sensor_df['consumed_unit'] - sensor_df['consumed_unit'].mean()) / sensor_df[
                    #     'consumed_unit'].std())

                    # outliers dataframe rows storing in dictionary to handle outliers
                    # outlier_dict['mean'] = sensor_df[sensor_df['consumed_unit'] > sensor_df['consumed_unit'].mean() * 3]
                    # outlier_dict['IQR'] = sensor_df[sensor_df['consumed_unit'] > u_limit]
                    # outlier_dict['z_score_3'] = sensor_df[abs(z_scores) > 3]
                    # outlier_dict['z_score_4'] = sensor_df[abs(z_scores) > 4]

                    # using that method which has the least number of outliers
                    # l1 = []
                    # for lst in outlier_dict.values():
                    #     l1.append(len(lst))
                    # min_length = min(l1)
                    # indices = [key for key, value in outlier_dict.items() if len(value) == min_length]

                    # # filling nan on places of outliers and filling the previous value
                    # sensor_df.loc[outlier_dict[indices[0]].index, 'consumed_unit'] = np.nan
                    # sensor_df.bfill(inplace=True)

                    # adding multiple values from sensor data
                    sensor_df['site_id'] = s_df['site_id']
                    sensor_df.reset_index(inplace=True)

                    # adding weather data and holidays
                    site = sensor_df['site_id']
                    weather_data = data_from_weather_api(site, startDate, endDate)

                    if not weather_data.empty:
                        weather_data['time'] = pd.to_datetime(weather_data['time'])
                        weather_data.set_index('time', inplace=True)

                        weather_data = weather_data[~weather_data.index.duplicated(keep='first')]
                        weather_data = weather_data.resample('15 min').ffill()

                        # Convert the creation_time columns to datetime if they are not already
                        weather_data.reset_index(inplace=True)
                        weather_data['creation_time'] = pd.to_datetime(weather_data['time'])
                        sensor_df['creation_time'] = pd.to_datetime(sensor_df['creation_time'])
                        merged_df = weather_data.merge(sensor_df, on='creation_time', how="inner")

                        # Drop the redundant columns
                        merged_df.drop(
                            columns=['UOM', 'prev_KWh', 'opening_KWh', '_id_x', '_id_y',
                                     'site_id_y', 'site_id_x', 'creation_time_iso', 'time'], inplace=True)
                    else:
                        print("Weather data is empty. Skipping merge operation or performing alternative action...")
                        continue

                    merged_df.set_index(['creation_time'], inplace=True, drop=True)
                    dfresample = add_lags(merged_df)
                    dfresample = create_features(dfresample)
                    dfresample.reset_index(inplace=True)
                    test_size = 24 * 7  # 7 days * 24 hours/day
                    gap = 24  # 1 day * 24 hours/day

                    total_size = test_size + gap

                    tss = TimeSeriesSplit(n_splits=5, test_size=total_size, gap=gap)

                    for train_idx, val_idx in tss.split(dfresample):
                        train_data = dfresample.iloc[train_idx]
                        test_data = dfresample.iloc[val_idx]

                    train_data_filepath = os.path.join(self.config.root_dir, f"train_data_sensor_{s_id}.csv")
                    test_data_filepath = os.path.join(self.config.root_dir, f"test_data_sensor_{s_id}.csv")
                    data_filepath = os.path.join(self.config.data_file_path, f"sensor_{s_id}.csv")

                    # Save data to separate train and test files for each sensor
                    train_data.to_csv(train_data_filepath, mode='w', header=True, index=False)
                    test_data.to_csv(test_data_filepath, mode='w', header=True, index=False)
                    dfresample.to_csv(data_filepath, mode='w', header=True, index=False)

    except Exception as e:
        print(traceback.format_exc())
        logger.info(f"Error occur in Data Transformation Layer {e}")


def sensorDecode(self, sensor_ids) -> None:
    try:
        decoded_values = self.le.inverse_transform(sensor_ids)
        encoded_to_sensor_mapping = {str(encoded_value): str(original_sensor_id) for
                                     encoded_value, original_sensor_id in
                                     zip(sensor_ids, decoded_values)}

        # Print the mapping
        print("Encoded to Sensor ID Mapping:")
        for encoded_value, original_sensor_id in encoded_to_sensor_mapping.items():
            print(f"Encoded Value {encoded_value} corresponds to Sensor ID {original_sensor_id}")

        # Write the mapping to a JSON file
        output_file_path = 'encoded_to_sensor_mapping.json'
        with open(output_file_path, 'w') as file:
            json.dump(encoded_to_sensor_mapping, file)

    except ValueError as e:
        # Handle the case where unknown values are encountered
        print(f"Error decoding sensor IDs: {e}")
