from django.http import HttpRequest
from rest_framework.views import APIView
from rest_framework.authtoken.models import Token
from .serializers import UserSerializer, UserAuthenticationSerializer
from rest_framework.response import Response
from rest_framework.exceptions import AuthenticationFailed
from rest_framework.permissions import IsAuthenticated
from .models import User
import jwt
from datetime import datetime

# Create your views here.


class RegisterView(APIView):
    serializer_class = UserSerializer

    def post(self, request: HttpRequest):
        serializer = UserSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data)


class LoginView(APIView):
    serializer_class = UserAuthenticationSerializer

    def post(self, request: HttpRequest):
        serializer = UserAuthenticationSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        user = User.objects.filter(email=serializer.validated_data.get("email")).first()
        if user is None:
            raise AuthenticationFailed("User not found")

        if not user.check_password(serializer.validated_data.get("password")):
            raise AuthenticationFailed("Password is incorrect")

        token, _ = Token.objects.get_or_create(user=user)

        response = Response()
        response.set_cookie("session", token.key, httponly=True)
        response.set_cookie("logged_in", True)

        response.data = {"token": token.key}
        return response


class LogoutView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: HttpRequest):
        Token.objects.delete(user=request.user)

        response = Response()
        response.delete_cookie("session")
        response.delete_cookie("logged_in")
        return response
