# <YOUR_IMPORTS>
import logging
import json
import os
import dill
import pandas as pd
from datetime import datetime

# Укажем путь к файлам проекта:
# -> $PROJECT_PATH при запуске в Airflow
# -> иначе - текущая директория при локальном запуске
path = os.environ.get('PROJECT_PATH', '..')


# Функция получения списка наименований файлов в каталоге
def get_filename(dir_path):
    for root, dir, files in os.walk(dir_path):
        return files


# Функция предсказания категории стоимости одного автомобиля
def predict_category(input_dict, model):
    df = pd.json_normalize(input_dict)
    y = model.predict(df)
    return y


def predict():
    # Не понятно зачем для предикта загружать датасет - закомментируем
    # df = pd.read_csv('data/homework.csv')
    # Напишем другую строку загрузки модели, поэтому эту закомментируем
    # with open('cars_pipe.pkl', 'rb') as file:
    #    model = dill.load(file)
    # <YOUR_CODE>

    # Получаем наименование последнего сформированного файла модели в каталоге models
    model_file_names_list = get_filename(f'{path}/data/models')
    logging.info(f'model filename list: {type(model_file_names_list)} {model_file_names_list}')
    last_model_file_name = model_file_names_list[-1]
    logging.info(f'last model filename in list: {last_model_file_name}')
    # Загружаем модель
    with open(f'{path}/data/models/{last_model_file_name}', 'rb') as pkl:
        model = dill.load(pkl)
    # Объявляем переменную-список, в которую будем записывать предсказания
    pred = []
    # Обходим все файлы с данными автомобилей в каталоге test
    test_path =  path + "/data/test"
    for file in get_filename(test_path):
        with open(f'{test_path}/{file}') as json_file:
            data = json.load(json_file)
            pred.append([file[:-5], predict_category(data, model)[0]])
    # Записываем результат предсказаний в датафрейм и в csv файл
    res_df = pd.DataFrame(pred, columns=['car_id', 'pred'])
    csv_filename = f'{path}/data/predictions/preds_{datetime.now().strftime("%Y%m%d%H%M")}.csv'
    res_df.to_csv(csv_filename, index=False)


if __name__ == '__main__':
    predict()
