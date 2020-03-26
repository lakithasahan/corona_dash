import json

from django.http import JsonResponse
from django.shortcuts import render

from corona_dash_core.corona_main import corona_dash_class


def index_view(request):
    corona_obj=corona_dash_class()

    return render(request, 'index.html')

def get_data(request):
    corona_obj = corona_dash_class()
    json_converted=corona_obj.combined()
    json_converted1 = corona_obj.combined_table()
    json_converted2 = corona_obj.precentage_of_death_by_age()

    data={'data1':json_converted,'data2':json_converted1,'data3':json_converted2}


    #print(json_converted)



    return JsonResponse(json.dumps(data), safe=False)



