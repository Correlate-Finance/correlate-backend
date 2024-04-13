from django.urls import path
from .views import (
    RegisterView,
    LoginView,
    LogoutView,
    AddWatchListView,
    WatchlistedView,
    DeleteWatchListView,
)

urlpatterns = [
    path("register", RegisterView.as_view(), name="register"),
    path("register/", RegisterView.as_view()),
    path("login", LoginView.as_view(), name="login"),
    path("login/", LoginView.as_view()),
    path("logout", LogoutView.as_view(), name="logout"),
    path("logout/", LogoutView.as_view(), name="logout"),
    path("watchlisted", WatchlistedView.as_view(), name="is-clicked"),
    path("addwatchlist", AddWatchListView.as_view(), name="add-watchlist"),
    path("addwatchlist/", AddWatchListView.as_view(), name="add-watchlist"),
    path("deletewatchlist", DeleteWatchListView.as_view(), name="delete-watchlist"),
    path("deletewatchlist/", DeleteWatchListView.as_view(), name="delete-watchlist"),
]
