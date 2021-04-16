from __future__ import print_function
import requests
from jproperties import Properties

import time
import generic_camunda_client
from generic_camunda_client.rest import ApiException


def get_weather(city):
    config_dict = get_configs('WeatherAPIConfig.properties')
    q = ''
    if city == 'Helsinki':
        q = '60.1699,24.9384'
    elif city == 'Aijala':
        q = '60.1921,23.3500'
    elif city == 'Frankfurt am Main':
        q = '50.1109,8.6821'
    else:
        print('No city defined')
        return
    url = config_dict.get('BaseURL') + '/current.json?key=' + config_dict.get('APIKey') + '&q=' + q
    r = requests.get(url)
    return r.json()


def get_configs(file_name):
    configs = Properties()
    with open(file_name, 'rb') as config_file:
        configs.load(config_file)
    items_view = configs.items()
    configs_dict = {}

    for item in items_view:
        configs_dict[item[0]] = item[1].data
    return configs_dict


def run_get_weather():
    config_dict = get_configs('CamundaAPIConfig.properties')
    fetch_and_lock_payload = {"workerId": "getWeatherWorker",
                              "maxTasks": 1,
                              "usePriority": "true",
                              "topics":
                                  [{"topicName": "GetHelsinkiWeather",
                                    "lockDuration": 30000
                                    },
                                   {"topicName": "GetAijalaWeather",
                                    "lockDuration": 30000
                                    },
                                   {"topicName": "GetFrankfurtWeather",
                                    "lockDuration": 30000
                                    }
                                   ]
                              }

    host = config_dict.get('BaseURL')
    configuration = generic_camunda_client.Configuration(host)

    # Enter a context with an instance of the API client
    with generic_camunda_client.ApiClient(configuration) as api_client:
        api_instance = generic_camunda_client.ExternalTaskApi(api_client)

        try:
            api_response = api_instance.fetch_and_lock(fetch_external_tasks_dto=fetch_and_lock_payload)
            while not api_response:
                time.sleep(5)
                api_response = api_instance.fetch_and_lock(fetch_external_tasks_dto=fetch_and_lock_payload)
                print('Fetch and lock response: ', api_response)

                if api_response:
                    break
            task_id = api_response[0].id
            city_name = api_response[0].variables.get('cityName').value
        except ApiException as e:
            print("Exception when calling ExternalTaskApi->fetch_and_lock: %s\n" % e)
            return

        try:
            weather = get_weather(city_name)
            print(weather)
            complete_external_task_dto = {"workerId": "getWeatherWorker",
                                          "variables": {
                                              "weather": {"type": "String","value": weather}}}  # CompleteExternalTaskDto |  (optional)
            api_response = api_instance.complete_external_task_resource(task_id,
                                                                        complete_external_task_dto=complete_external_task_dto)
        except ApiException as e:
            print("Exception when calling ExternalTaskApi->complete_external_task_resource: %s\n" % e)


if __name__ == '__main__':
    try:
        while True:
            run_get_weather()
            time.sleep(15)
    except KeyboardInterrupt:
        pass
