from django.shortcuts import render
from django.views.generic.base import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin

def index(request):
    return render(request, 'index.html')

# Login required
class DataBaseView(LoginRequiredMixin, TemplateView):
    template_name = 'dataBase.html'

# Login required
class AnalysisView(LoginRequiredMixin, TemplateView):
    template_name = 'analysis.html'


