"""
URL configuration for abschlussarbeit project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
# abschlussarbeit/urls.py

from django.contrib import admin
from django.urls import path, include
from django.contrib.auth import views as auth_views 

from device_overview.views import IndexView

urlpatterns = [
    path("admin/", admin.site.urls),

    # Startseite
    path("", IndexView.as_view(), name="index"),

    # Device-App
    path("", include("device_overview.urls")),

    # Django Auth mit eigenen Templates
    path("accounts/login/",  auth_views.LoginView.as_view(
        template_name="accounts/login.html"
    ), name="login"),
    path("accounts/logout/", auth_views.LogoutView.as_view(
        next_page="index"   # oder wo du nach Logout hin willst
    ), name="logout"),
]
