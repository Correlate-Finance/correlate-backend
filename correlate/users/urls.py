from django.urls import path
from .views import (
    RegisterView,
    LoginView,
    LogoutView,
    AddWatchListView,
    DeleteWatchListView,
    SendOTPView,
    VerifyOTPView,
    ChangePasswordView,
)

urlpatterns = [
    path("register", RegisterView.as_view(), name="register"),
    path("register/", RegisterView.as_view()),
    path("login", LoginView.as_view(), name="login"),
    path("login/", LoginView.as_view()),
    path("logout", LogoutView.as_view(), name="logout"),
    path("logout/", LogoutView.as_view(), name="logout"),
    path("addwatchlist", AddWatchListView.as_view(), name="add-watchlist"),
    path("addwatchlist/", AddWatchListView.as_view(), name="add-watchlist"),
    path("deletewatchlist", DeleteWatchListView.as_view(), name="delete-watchlist"),
    path("deletewatchlist/", DeleteWatchListView.as_view(), name="delete-watchlist"),
    path("send-otp", SendOTPView.as_view(), name="send-otp"),
    path("send-otp/", SendOTPView.as_view(), name="send-otp"),
    path("verify-otp", VerifyOTPView.as_view(), name="verify-otp"),
    path("verify-otp/", VerifyOTPView.as_view(), name="verify-otp"),
    path ("change-password", ChangePasswordView.as_view(), name="change-password"),
    path ("change-password/", ChangePasswordView.as_view(), name="change-password"),
]
