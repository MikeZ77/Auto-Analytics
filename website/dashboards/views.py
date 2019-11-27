from django.shortcuts import render
from django.http import HttpResponse
from django.http import HttpRequest
from django.http import JsonResponse
from django.template import loader
from dashboards.models import main
from django.http import JsonResponse
from django.utils.safestring import SafeString
# Create your views here.

def index(request):
    type_list = main.objects.values('body_type').distinct().order_by('body_type')
    context = {'types':type_list}
    return render(request,'dashboards/home_form.html',context)

def make_choices_ajax(request):
    choice = request.GET.get('id')
    makes_list = main.objects.filter(body_type=choice).values_list('make', flat=True).distinct().order_by('make')
    context = {'makes':makes_list}
    return render(request,'dashboards/make_dropdown.html',context)

def model_choices_ajax(request):
    choice = request.GET.get('id')
    choices = choice.split('/')
    model_list = main.objects.filter(body_type=choices[0], make=choices[1]).values_list('model', flat=True).distinct().order_by('model')
    context = {'models':model_list}
    return render(request,'dashboards/model_dropdown.html',context)

def year_choices_ajax(request):
    print(request)
    choice = request.GET.get('id')
    choices = choice.split('/')
    year_list = main.objects.filter(body_type=choices[0], make=choices[1], model=choices[2]).values_list('year', flat=True).distinct().order_by('-year')
    context = {'years':year_list}
    return render(request,'dashboards/year_dropdown.html',context)

def validate_dashboard(request):
    #default json
    data ={
        'type': request.GET['type'],
        'make':request.GET['make'],
        'model':request.GET['model'],
        'year':request.GET['year'],
        'success':True,
        'error':None
    }
    #form handling
    if request.GET['type']=='':
        data['error']='type'
    elif request.GET['make']=='':
        data['error']='make'
    elif request.GET['model']=='':
        data['error']='model'
    elif request.GET['year']=='':
        data['error']='year'

    if data['error']!=None: data['success']=False

    return JsonResponse(data)

def dashboard_display(request,make,model,year):

    context = {'data':'{"vehicle_data":{"value":"'+make+' '+model+' '+year+'"}}',
                'dropdown_link':request.path,
                'dropdown_name':make+' '+model+' '+year,
                }
    return render(request,'dashboards/descriptive_tmp.html',context)

def update_link_cookies(request):
    print(request.GET.get('link', False))
    print(request.GET.get('name_link', False))
    print(request.method)

    if request.method=='GET':

        response = HttpResponse("SET COOKIE")
        #First check if cookie exists
        for i in range(1,7):
            try:
                if request.COOKIES['dashboard_link_'+ str(i)] == request.GET.get('link', False):
                    return HttpResponse()
            except KeyError:
                break
        #Check which links are available (max 6)
        for i in range(1,7):
            try:
                request.COOKIES['dashboard_link_'+ str(i)]
            except KeyError:
                response.set_cookie('dashboard_link_'+ str(i),request.GET.get('link', False))
                response.set_cookie('dashboard_name_'+ str(i),request.GET.get('name_link', False))

                return response
        #Overide the oldest saved dashboard (reorder)
        for i in range(1,7-1):
            response.set_cookie('dashboard_link_'+ str(i),request.COOKIES['dashboard_link_'+ str(i+1)])
            response.set_cookie('dashboard_name_'+ str(i),request.COOKIES['dashboard_name_'+ str(i+1)])

        response.set_cookie('dashboard_link_6',request.GET.get('link', False))
        response.set_cookie('dashboard_name_6',request.GET.get('name_link', False))

        return response
