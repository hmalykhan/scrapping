from django.urls import path
from .views import embed

urlpatterns = [
    path("embed/", embed),
]