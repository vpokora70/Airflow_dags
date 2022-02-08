import requests
import json
from requests.exceptions import ConnectionError, HTTPError, RequestException
from hdfs import InsecureClient
import os
from datetime import datetime
from datetime import date
from config import Config


def ap_hd(process_date='2021-12-10'):
    # read configuration
    try:
        config = Config('/home/user/airflow/dags/common/config.yaml').get_config()
    except:
        print("On config.yaml error ")
        raise Exception("On config.yaml error ")

    url = config['API']['url']
    headers = {"content-type": "application/json"}
    data = {"username": config['API']['username'], "password": config['API']['password']}
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print(response.status_code)
        response.raise_for_status()
        token = response.json()["access_token"]
    except HTTPError:
        print('Wrong endpoint')
    except RequestException:
        raise Exception("Error with API")

    if process_date:
        date_name = config['AUTH']['date']
        #date_name.append(process_date)
    else:
        # если process_date= None создается папка для текущей даты и затягиватемся данные
        date_name = [str(date.today())]
    for dir_name in date_name:

        path_to_directory = os.path.join(config['HP']['h_path'], dir_name)
        file_name = config['AUTH']['file_name']
        url2 = config['AUTH']['url']
        headers2 = {"content-type": "application/json",
                    "Authorization": config['AUTH']['Authorization'] + token}

        data2 = {"date": dir_name}
        try:
            response2 = requests.get(url2, headers=headers2, data=json.dumps(data2))
            print(response2.status_code, data2)
            data = response2.json()
        except RequestException:
            print("Got an error during calling 'Exchange rate API'")
            raise Exception("Error with API")
        time_file = datetime.today().strftime('%d-%H-%M')
        client = InsecureClient(url=config['HP']['h_server'], user=config['HP']['h_user'])
        client.makedirs(path_to_directory)
        with client.write(os.path.join(f'{path_to_directory}', f'{time_file}-{file_name}'), encoding='utf-8') as json_file:
            json.dump(data, json_file)



if __name__ == '__main__':
    ap_hd()

