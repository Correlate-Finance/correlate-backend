from django.urls import path
from .views import (
    RegisterView,
    LoginView,
    LogoutView,
    AddWatchListView,
    DeleteWatchListView,
)

urlpatterns = [
    path("register", RegisterView.as_view()),
    path("register/", RegisterView.as_view()),
    path("login", LoginView.as_view()),
    path("login/", LoginView.as_view()),
    path("logout", LogoutView.as_view(), name="logout"),
    path("logout/", LogoutView.as_view(), name="logout"),
    path("addwatchlist", AddWatchListView.as_view(), name="add-watchlist"),
    path("addwatchlist/", AddWatchListView.as_view(), name="add-watchlist"),
    path("deletewatchlist", DeleteWatchListView.as_view(), name="delete-watchlist"),
    path("deletewatchlist/", DeleteWatchListView.as_view(), name="delete-watchlist"),
]
