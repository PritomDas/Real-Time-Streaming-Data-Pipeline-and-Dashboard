from django.shortcuts import render

from django.http import JsonResponse
from dashboard.models import event_message_detail_agg_tbl
from django.core import serializers

# Create your views here.

def dashboard(request):
    return render(request, 'dashboard.html', {})

def dashboard_data(request):
    dataset = event_message_detail_agg_tbl.objects.all()
    data = serializers.serialize('json', dataset)
    return JsonResponse(data, safe=False)
