from django.urls import path
from . import views

app_name = 'dashboards'

urlpatterns = [
    path('', views.index, name='index'),
    path('make',views.make_choices_ajax,name='make_choices_ajax'),
    path('model',views.model_choices_ajax,name='model_choices_ajax'),
    path('year',views.year_choices_ajax,name='year_choices_ajax'),
    path('validate',views.validate_dashboard,name='validate_dashboard'),
    path('links',views.update_link_cookies,name='update_link_cookies'),
    path('dashboard/descriptive=<str:descriptive>/value=<str:value>/<str:type>/<str:make>/<str:model>/<str:year>/',views.dashboard_display,name='dashboard_display'),
]
