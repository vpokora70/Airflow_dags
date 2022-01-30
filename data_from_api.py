import requests
import json

import os
from datetime import date



def ap(**kwargs):

    url1 = 'https://robot-dreams-de-api.herokuapp.com/auth'
    data = {'username': "rd_dreams", 'password': "djT6LasE"}
    headers1 = {'content-type': 'application/json'}
    response = requests.post(url=url1, headers=headers1, data=json.dumps(data))
    print(response.status_code)
    token = response.json()["access_token"]

    store_folder = '2021-12-08'
    # store_folder = str(date.today())
    path_to_directory = os.path.join('/home/user/shared_folder/data', store_folder)

    os.makedirs(path_to_directory, exist_ok=True)
    url2 = 'https://robot-dreams-de-api.herokuapp.com/out_of_stock'
    headers2 = {"content-type": "application/json", "Authorization": "JWT " + token}
    data2 = {'date': store_folder}
    response2 = requests.get(url2, headers=headers2, data=json.dumps(data2))
    print(response2.status_code, store_folder)
    data = response2.json()

    file_name = 'stock.json'
    with open(os.path.join(path_to_directory, file_name), 'w') as json_file:
        json.dump(data, json_file)


if __name__ == '__main__':
    ap()
