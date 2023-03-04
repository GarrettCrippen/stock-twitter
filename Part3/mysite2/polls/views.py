from django.http import HttpResponse
from django.template import loader
from django.views.generic import ListView
from polls.pyecharts import JsonResponse, chart1_base, chart2_base, chart3_base, chart4_base
from polls.plotly import plotly_chart
from django.shortcuts import render
from django.views.generic import TemplateView
from django.views.decorators.csrf import csrf_exempt
import json


@csrf_exempt
def index(request):
   context = {'segment': 'index'}
   html_template = loader.get_template('index.html')
   return HttpResponse(html_template.render(context, request))

def chart1(request):
   context = {'segment': 'chart1'}
   html_template = loader.get_template('chart1.html')
   return HttpResponse(html_template.render(context, request))


class ChartView1(ListView):
   def get(self, request, *args, **kwargs):
       return JsonResponse(json.loads(chart1_base()))
    
def chart2(request):
   context = {'segment': 'chart2'}
   html_template = loader.get_template('chart2.html')
   return HttpResponse(html_template.render(context, request))

class ChartView2(ListView):
   def get(self, request, *args, **kwargs):
       return JsonResponse(json.loads(chart2_base()))
    
def chart3(request):
   context = {'segment': 'chart3'}
   html_template = loader.get_template('chart3.html')
   return HttpResponse(html_template.render(context, request))

def chart4(request):
   context = {'segment': 'chart4'}
   html_template = loader.get_template('chart4.html')
   return HttpResponse(html_template.render(context, request))


class ChartView3(ListView):
   def get(self, request, *args, **kwargs):
       return JsonResponse(json.loads(chart3_base()))

class ChartView4(ListView):
   def get(self, request, *args, **kwargs):
       return JsonResponse(json.loads(chart4_base()))  

@csrf_exempt    
def chartP(request):
    return plotly_chart(request)




