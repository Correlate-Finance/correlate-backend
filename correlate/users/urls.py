from django.urls import path
from .views import RegisterView, LoginView, HomeView, LogoutView
from rest_framework_simplejwt import views as jwt_views

urlpatterns = [
    path("register/", RegisterView.as_view()),
    path("login/", LoginView.as_view()),
    path("logout/", LogoutView.as_view(), name="logout"),
    # path("token/", jwt_views.TokenObtainPairView.as_view(), name="token_obtain_pair"),
    # path("token/refresh/", jwt_views.TokenRefreshView.as_view(), name="token_refresh"),
    path("home/", HomeView.as_view(), name="home"),
]
