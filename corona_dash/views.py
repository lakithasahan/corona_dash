from django.http import JsonResponse
from django.shortcuts import render

from corona_dash_core.corona_main import corona_dash_class


def index_view(request):
    corona_obj=corona_dash_class()
    return render(request, 'index.html')

def get_data(request):
    corona_obj = corona_dash_class()
    corona_obj.get_confirmed_cases_time_series()
    json_converted=corona_obj.get_location_data()

    return JsonResponse(json_converted, safe=False)



