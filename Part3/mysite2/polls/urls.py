from django.urls import path, re_path
from django.views.generic import TemplateView
from . import views


urlpatterns = [
    path('', views.index, name='index'),
    path(r'chart1/', views.chart1, name='chart1'),
    path(r'chart1_data/', views.ChartView1.as_view(), name='chart1_data'),
    path(r'chart2/', views.chart2, name='chart2'),
    path(r'chart2_data/', views.ChartView2.as_view(), name='chart2_data'),
    path(r'chart3/', views.chart3, name='chart3'),
    path(r'chart3_data/', views.ChartView3.as_view(), name='chart3_data'),
    path(r'chart4/', views.chart4, name='chart4'),
    path(r'chart4_data/', views.ChartView4.as_view(), name='chart4_data'),
    re_path(r'stocks/', views.chartP, name='stocks'),
    
]
