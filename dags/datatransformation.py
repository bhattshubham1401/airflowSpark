import os
from datetime import timedelta, datetime

import holidays
import numpy as np
import pandas as pd
from utils.mongodbHelper import data_from_weather_api, load_data


def init_transformation(sensor_data, site_id, circle_id, precomputed_holidays=None, start_date=None, end_date=None):
    df = pd.DataFrame(sensor_data)
    if start_date is None or end_date is None:
        # Set default values
        start_date = datetime.strptime('2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime('2024-03-30 23:59:59', '%Y-%m-%d %H:%M:%S')
    if precomputed_holidays is None:
        holiday = holidays_list(start_date, end_date)

    df['creation_time'] = pd.to_datetime(df['creation_time'])
    df['is_holiday'] = df['creation_time'].dt.date.isin(holiday).astype(int)
    df = df[(df['creation_time'] >= start_date) & (df['creation_time'] <= end_date)]
    df.set_index('creation_time', inplace=True)
    filtered_df = df[df['opening_KWh'] > df['closing_KWh']]
    filter_ratio = 0.01
    if len(filtered_df) < filter_ratio * len(df):
        df.loc[filtered_df.index, 'opening_KWh'] = np.nan

    df.loc[(df['R_Current'] == 0) & (df['B_Current'] == 0) & (df['Y_Current'] == 0), 'opening_KWh'] = np.nan
    df.loc[(df['R_Voltage'] == 0) & (df['Y_Voltage'] == 0) & (df['B_Voltage'] == 0) &
           ((df['R_Current'] != 0) | (df['B_Current'] != 0) | (df['Y_Current'] != 0)), 'opening_KWh'] = np.nan

    if df['opening_KWh'].nunique() < 10:
        pass

    # previous value of opening_KWh
    df['prev_KWh'] = df['opening_KWh'].shift(1)
    df.dropna(inplace=True)
    if len(df[df['prev_KWh'] > df['opening_KWh']]) > 25:
        pass
    df['consumed_unit'] = df['opening_KWh'] - df['prev_KWh']
    df.loc[df['consumed_unit'] < 0, "opening_KWh"] = df["prev_KWh"]
    df.loc[df['consumed_unit'] < 0, "consumed_unit"] = 0

    if df['consumed_unit'].nunique() < 10:
        pass
    Q1 = df['consumed_unit'].quantile(0.25)
    Q3 = df['consumed_unit'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - (1.5 * IQR)
    upper_bound = Q3 + (1.5 * IQR)
    df = df[(df['consumed_unit'] >= lower_bound) & (df['consumed_unit'] <= upper_bound)]

    # if len(df) > 3000:
    df1 = add_lags(df)
    df2 = create_features(df1)
    df2.reset_index(inplace=True)
    pd.set_option('display.max_columns', 500)

    try:
        weather_data = data_from_weather_api(site_id, start_date, end_date)
        if not weather_data.empty:
            weather_data['time'] = pd.to_datetime(weather_data['time'])
            weather_data.set_index('time', inplace=True)

            weather_data = weather_data[~weather_data.index.duplicated(keep='first')]
            weather_data = weather_data.resample('15 min').ffill()

            # Convert the creation_time columns to datetime if they are not already
            weather_data.reset_index(inplace=True)
            weather_data['creation_time'] = pd.to_datetime(weather_data['time'])
            df2['creation_time'] = pd.to_datetime(df2['creation_time'])
            # return weather_data, df2
            merged_df = weather_data.merge(df2, on='creation_time', how="inner")
            merged_df.drop(
                columns=['time', '_id_x', 'creation_time_iso', '_id_y',
                         'instant_cum_KW', 'instant_R_KW', 'instant_Y_KW',
                         'instant_B_KW', 'instant_cum_KVA', 'instant_R_KVA', 'instant_Y_KVA',
                         'instant_B_KVA', 'R_Voltage', 'Y_Voltage', 'B_Voltage', 'R_Current',
                         'Y_Current', 'B_Current', 'R_PF', 'Y_PF', 'B_PF', 'cumm_PF', 'status',
                         'creation_time_ms', 'opening_KWh', 'opening_KVAh',
                         'closing_KWh', 'closing_KVAh', 'opening_leading_value',
                         'opening_lagging_value', 'closing_leading_value',
                         'closing_lagging_value', 'no_e', 'no_p', 'no_o',
                         'prev_KWh'], inplace=True)
            mongo_dict = merged_df.to_dict(orient='records')
            load_data(mongo_dict, circle_id)
            return "success"

    except Exception as e:
        print(e)


def holidays_list(start_date_str, end_date_str):
    try:
        start_date = start_date_str.date()
        end_date = end_date_str.date()
        holiday_list = []
        india_holidays = holidays.CountryHoliday('India', years=start_date.year)
        current_date = start_date
        while current_date <= end_date:
            if current_date in india_holidays or current_date.weekday() == 6:
                holiday_list.append(current_date)
            current_date += timedelta(days=1)
        return holiday_list
    except Exception as e:
        return None


# def holidays_list(start_date, end_date):
#   try:
#       india_holidays = holidays.CountryHoliday('India', years=start_date.year)
#       return [d for d in range(start_date, end_date + timedelta(days=1)) if d in india_holidays or d.weekday() == 6]
#   except Exception as e:
#       return None

def add_lags(df):
    try:
        target_map = df['consumed_unit'].to_dict()
        # 15 minutes, 30 minutes, 1 hour
        df['lag1'] = (df.index - pd.Timedelta('15 minutes')).map(target_map)
        df['lag2'] = (df.index - pd.Timedelta('30 minutes')).map(target_map)
        df['lag3'] = (df.index - pd.Timedelta('1 day')).map(target_map)
        df['lag4'] = (df.index - pd.Timedelta('7 days')).map(target_map)
        df['lag5'] = (df.index - pd.Timedelta('15 days')).map(target_map)
        df['lag6'] = (df.index - pd.Timedelta('30 days')).map(target_map)
        df['lag7'] = (df.index - pd.Timedelta('45 days')).map(target_map)
    except KeyError as e:
        print(f"Error: {e}. 'consumed_unit' column not found in the DataFrame.")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")

    return df


def create_features(hourly_data):
    hourly_data = hourly_data.copy()

    # Check if the index is in datetime format
    if not isinstance(hourly_data.index, pd.DatetimeIndex):
        hourly_data.index = pd.to_datetime(hourly_data.index)

    hourly_data['day'] = hourly_data.index.day
    hourly_data['hour'] = hourly_data.index.hour
    hourly_data['month'] = hourly_data.index.month
    hourly_data['dayofweek'] = hourly_data.index.dayofweek
    hourly_data['quarter'] = hourly_data.index.quarter
    hourly_data['dayofyear'] = hourly_data.index.dayofyear
    hourly_data['weekofyear'] = hourly_data.index.isocalendar().week
    hourly_data['year'] = hourly_data.index.year
    return hourly_data
