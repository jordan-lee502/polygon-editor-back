# authx/urls.py
from django.urls import path
from .views import SendCode, Login, Refresh, Logout, Me

urlpatterns = [
    path("send-code/", SendCode.as_view(), name="send_code"),
    path("login/",     Login.as_view(),     name="login"),
    path("refresh/",   Refresh.as_view(),   name="refresh"),
    path("logout/",    Logout.as_view(),    name="logout"),
    path("me/",        Me.as_view(),        name="me"),
]
