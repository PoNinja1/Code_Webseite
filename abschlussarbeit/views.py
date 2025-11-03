from django.shortcuts import render

def index(request):
    return render(request, 'index.html')

def dataBase(request):
    return render(request, 'dataBase.html')

def XXX(request):
    return render(request, 'XXX.html')
