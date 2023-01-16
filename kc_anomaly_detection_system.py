import pandas as pd
import numpy as np
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import datetime as dt
import io
import time as t
import sql_requests as sqr

from airflow.decorators import dag, task

palette = ['#6D9BC3', '#C0362C', '#505050', '#00937F','#E7DD73', 
           '#7B5141', '#586BA4', '#B86A84', '#587B7F', '#ABB7C1']

custom_params = {
    "figure.figsize": (15, 7), # ширина и высота в дюймах
    "axes.titlesize": 13, # заголовок
    "axes.labelsize": 13, # оси
    "xtick.labelsize": 11, # деления оси X
    "ytick.labelsize": 11, # деления оси Y
    'axes.spines.left': False, # не отображать ось Y
    'axes.spines.right': False, # не отображать правую рамку 
    'axes.spines.top': False, # не отображать верхнюю рамку
    'grid.color': '.8',
    'grid.linestyle': ':',
    'lines.linewidth': 1.5,
}

sns.set_theme(style='whitegrid', palette=palette, rc=custom_params)

pd.set_option('display.max_rows', 6) # число отображаемых строк по умолчанию (здесь head(3), и tail(3))
pd.set_option('display.max_columns', 20) # число отображаемых столбцов по умолчанию (здесь 20)

schedule_interval = '*/15 * * * *'

chat_id = # введите ID чата, в который необходимо отправлять сообщения
distance = 3

token = # токен бота телеграм
bot = telegram.Bot(token=token)

def load_df(query=query, connection=connection):
    df = ph.read_clickhouse(query=query, connection=connection)
    
    return df

def create_dict_var(df):
    
    dict_var = {}
    
    dict_var['round_to_0'] = df.columns.drop(['date', 'time_window', 'ctr']).to_list()
    dict_var['round_to_3'] = 'ctr'
    dict_var['feed'] = df.columns.drop(['date', 'time_window', 'mes', 'users_mes']).to_list()
    dict_var['mes'] = ['mes', 'users_mes']
    
    return dict_var

def boundaries(df, variable, distance=1.5):
    
    iqr = df[variable].quantile(0.75) - df[variable].quantile(0.25)
    mean = df[variable].mean()
    
    lower_boundary = df[variable].quantile(0.25) - (iqr * distance)
    upper_boundary = df[variable].quantile(0.75) + (iqr * distance)
    
    return lower_boundary, upper_boundary, mean


def check_if_outlier(df, variable, distance=5):
    
    df1 = df[:-1]
    
    lower_boundary, upper_boundary, mean = boundaries(df1, variable, distance=distance)
    
    check = ((df[variable][-1:] < lower_boundary) | \
             (df[variable][-1:] > upper_boundary)).item()
    
    if check:
        return True, lower_boundary, upper_boundary, mean
    else:
        return False, lower_boundary, upper_boundary, mean

    
def dict_anomaly(df, distance=1.5):
    
    dict_anomalies = {
       'metric': [],
        'value': [],
        'mean': [],
        'lower': [],
        'upper': [],
        'deviation': [],
        'deviation_rel': []
    }

    for i in df.columns:
        
        check, lower_boundary, upper_boundary, mean = check_if_outlier(df, i, distance=distance)

        if check:

            deviation = df[i][-1:].item() - mean
            
            deviation_rel = (df[i][-1:].item() - mean) / mean * 100

            dict_anomalies['metric'].append(i)
            dict_anomalies['value'].append(df[i][-1:].item())
            dict_anomalies['mean'].append(mean)
            dict_anomalies['lower'].append(lower_boundary)
            dict_anomalies['upper'].append(upper_boundary)
            dict_anomalies['deviation'].append(deviation)
            dict_anomalies['deviation_rel'].append(deviation_rel)

    return dict_anomalies


def text_replace(text):
    
    text = text \
        .replace('users_feed', 'Пользователи ленты') \
        .replace('users_mes' , 'Пользователи мессенджера') \
        .replace('views', 'Просмотры') \
        .replace('likes', 'Лайки') \
        .replace('ctr', 'CTR') \
        .replace('mes', 'Сообщения') \
        .replace('value', 'Значение метрики') \
        .replace('lower', 'Нижний порог') \
        .replace('upper', 'Верхний порог') \
        .replace('.', '\\.') \
        .replace('-', '\\-') \
        .replace('{', '\\{') \
        .replace('}', '\\}') \
        .replace('(', '\\(') \
        .replace(')', '\\)')

    return text


def text_replace_df_columns(df):

    list_columns = []

    for i in df.columns.to_list():
        i = text_replace(i)
        list_columns.append(i)

    df.columns = list_columns

    return df


def bot_send_pic(chat_id, caption):
    
    plot_object = io.BytesIO()
    plt.savefig(plot_object, bbox_inches='tight')
    plot_object.seek(0)
    plot_object.name = 'metrics.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=caption, parse_mode='MarkdownV2')

    
def plot_anomaly(df, ylabel):
    
    sns.lineplot(
        x='Метка времени',
        y='Верхний порог',
        data=df,
        color=palette[1],
        linewidth=1,
        label='Верхний порог',
        linestyle='dashdot'
    );

    sns.lineplot(
        x='Метка времени',
        y='Значение метрики',
        data=df,
        color=palette[0],
        linewidth=3,
        label='Значение метрики'
    );

    sns.lineplot(
        x='Метка времени',
        y='Нижний порог',
        data=df,
        color=palette[1],
        linewidth=1,
        label='Нижний порог',
        linestyle='dashdot'
    );
    
    plt.xticks(rotation=45)
    plt.xticks(df['Метка времени'].to_list()[::3], fontsize=12)
    plt.xlabel('')
    plt.yticks(fontsize=12)
    plt.ylabel('')
    

def alert_anomaly(query, connection, dict_anomalies, dict_var, distance, chat_id):
    
    df_anomaly = ph.read_clickhouse(query=query, connection=connection)
    df_anomaly['date'] = df_anomaly['time_window'].dt.strftime('%y-%m-%d').copy()
    df_anomaly['time'] = df_anomaly['time_window'].dt.strftime('%H:%M').copy()

    df_anomaly_today = df_anomaly[df_anomaly['date'] == df_anomaly['date'][-1:].item()]
    df_anomaly_previous = df_anomaly[df_anomaly['date'] != df_anomaly['date'][-1:].item()]
    df_anomaly_previous_mean = df_anomaly_previous.groupby(['time']).mean().reset_index()

    list_time_stamps = df_anomaly_previous['time'].unique()

    df_final = pd.DataFrame(columns=[
        'Метрика',
        'Метка времени',
        'Верхний порог',
        'Значение метрики',
        'Нижний порог'
    ])
    
    for i in range(len(dict_anomalies['metric'])):

        metric = dict_anomalies['metric'][i]
        deviation_rel = round(dict_anomalies['deviation_rel'][i], 1)

        if metric in dict_var['round_to_0']:
            value_alert = round(dict_anomalies['value'][i])
            deviation = round(dict_anomalies['deviation'][i])

        if metric in dict_var['round_to_3']:
            value_alert = round(dict_anomalies['value'][i], 3)
            deviation = round(dict_anomalies['deviation'][i], 3)
        
        if metric in dict_var['feed']:
            alert = '⚠️ Aномальные значения' \
                    '\n' \
                    '\n_Метрика:_ *{0}*'\
                    '\n_Текущее значение:_ {1}'\
                    '\n_Абсолютное отклонение:_ {2}'\
                    '\n_Относительное отклонение:_ {3} %' \
                    '\n_Ссылка на дашборд:_ https://superset.lab.karpov.courses/superset/dashboard/2388/' \
                    '\n' \
                    '\n_Базовые значения:_ предыдущие 3 дня' \
                    '\n_Коэффициент пороговых значений:_ {4}' \
                    .format(metric, value_alert, deviation, deviation_rel, distance)
            
        if metric in dict_var['mes']:
            alert = '⚠️ Aномальные значения' \
                    '\n' \
                    '\n_Метрика:_ *{0}*'\
                    '\n_Текущее значение:_ {1}'\
                    '\n_Абсолютное отклонение:_ {2}'\
                    '\n_Относительное отклонение:_ {3} %' \
                    '\n_Ссылка на дашборд:_ https://superset.lab.karpov.courses/superset/dashboard/2388/' \
                    '\n' \
                    '\n_Базовые значения:_ предыдущие 3 дня' \
                    '\n_Коэффициент пороговых значений:_ {4}' \
                    .format(metric, value_alert, deviation, deviation_rel, distance)

        alert = text_replace(alert)

        dict_plot = {
            'Метрика': [],
            'Метка времени': [],
            'Верхний порог': [],
            'Значение метрики': [],
            'Нижний порог': []
                }

        for j in list_time_stamps:

            value = df_anomaly_today.loc[df_anomaly_today['time']==j, metric].item()

            df = df_anomaly_previous[df_anomaly_previous['time']==j]
            lower_boundary, upper_boundary = boundaries(df, metric, distance=distance)[:2]

            dict_plot['Метрика'].append(metric)
            dict_plot['Метка времени'].append(j)
            dict_plot['Верхний порог'].append(upper_boundary)
            dict_plot['Значение метрики'].append(value)
            dict_plot['Нижний порог'].append(lower_boundary)

        df_boundaries = pd.DataFrame(dict_plot)
        df_final = pd.concat([df_final, df_boundaries], axis=0)

        ylabel = text_replace(metric)

        plot_anomaly(
            df_final.loc[df_final['Метрика'] == metric],
            ylabel
        )

        bot_send_pic(chat_id=chat_id, caption=alert)


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def anomaly_system():

    
    @task()
    def load_data(query=query, connection=connection):
        df = load_df(query=query, connection=connection)
        return df

    
    @task
    def create_var_dictionary(df):
        dict_var = create_dict_var(df)
        return dict_var

    
    @task  
    def check(df, distance):
        dict_anomalies = dict_anomaly(df, distance=distance)
        return dict_anomalies
    

    @task
    def alert(query, connection, dict_anomalies, dict_var, distance, chat_id):
        
        if dict_anomalies:
            alert_anomaly(query, connection, dict_anomalies, dict_var, distance, chat_id)
        else: 
            pass
    
    
    df = load_data(query=sqr.query, connection=connection)

    dict_var = create_var_dictionary(df)
    
    dict_anomalies = check(df=df, distance=distance)
    
    alert(
        query=query_anomaly_15_min,
        connection=connection,
        dict_anomalies=dict_anomalies,
        dict_var=dict_var,
        distance=distance,
        chat_id=chat_id
    )
    

anomaly_system = anomaly_system()