import os
import sys

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

expanduser_path = os.path.expanduser('~')
path = '/opt/airflow'
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.pipeline import pipeline
# <YOUR_IMPORTS>
from modules.predict import predict


args = {
    'owner': 'airflow',                      # Информация о владельце DAG
    'start_date': datetime.now().strftime("%Y%m%d%H%M"),  # Время начала выполнения пайплайна
    'retries': 1,                            # Количество повторений в случае неудач
    'retry_delay': timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,                # Зависимость от успешного окончания предыдущего запуска
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="*/1 * * * *",
        default_args=args,
) as dag:
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
        dag=dag,
    )
    # <YOUR_CODE>
    predict = PythonOperator(
        task_id='predict',
        python_callable=predict,
        dag=dag,
    )

    pipeline >> predict