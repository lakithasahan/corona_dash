import json

from django.http import JsonResponse
from django.shortcuts import render

from corona_dash_core.corona_main import corona_dash_class


def index_view(request):
    corona_obj=corona_dash_class()

    return render(request, 'index.html')

def get_data(request):

    corona_obj = corona_dash_class()
    json_converted0=corona_obj.map_data()                       #geo map data
    json_converted1 = corona_obj.stat_table_data()              #stat_table_data
    json_converted2 = corona_obj.precentage_of_death_by_age()   #age_death_table data
    # json_converted3=corona_obj.time_seires_header_data()
    json_conveted3,json_converted4=corona_obj.world_wide_time_series()



    data={'data1':json_converted0,'data2':json_converted1,'data3':json_converted2,'data4':json_conveted3,'data5':json_converted4}

    return JsonResponse(json.dumps(data), safe=False)



def get_time_series_data(request):
    data_json = json.loads(request.body)
    country_name=data_json['country_name']
    print(country_name)
    corona_obj = corona_dash_class()
    json_converted3 = corona_obj.time_seires(country_name)


    return JsonResponse(json_converted3, safe=False)