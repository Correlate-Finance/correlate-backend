from django.http import HttpResponse
from rest_framework.request import Request
from rest_framework.views import APIView
from rest_framework.authtoken.models import Token
from .serializers import UserSerializer, UserAuthenticationSerializer
from rest_framework.response import Response
from rest_framework.exceptions import AuthenticationFailed
from rest_framework.permissions import IsAuthenticated
from .models import User, WatchList
from datetime import datetime, timedelta
import environ
from datasets import dataset_metadata


env = environ.Env()
environ.Env.read_env()


class RegisterView(APIView):
    serializer_class = UserSerializer

    def post(self, request: Request) -> HttpResponse:
        serializer = UserSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        return Response(serializer.data)


class LoginView(APIView):
    serializer_class = UserAuthenticationSerializer

    def post(self, request: Request) -> HttpResponse:
        serializer = UserAuthenticationSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        user = User.objects.filter(email=serializer.validated_data.get("email")).first()  # type: ignore
        if user is None:
            raise AuthenticationFailed("User not found")

        if not user.check_password(serializer.validated_data.get("password")):  # type: ignore
            raise AuthenticationFailed("Password is incorrect")

        token, _ = Token.objects.get_or_create(user=user)

        response = Response()
        response.set_cookie(
            "session",
            token.key,
            expires=datetime.utcnow() + timedelta(days=365),
            domain=".correlatefinance.com" if not env.bool("LOCAL_DEV") else None,
        )
        response.data = {"token": token.key}
        return response


class LogoutView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        Token.objects.get(user=request.user).delete()

        response = Response()
        response.delete_cookie(
            "session",
            domain=".correlatefinance.com" if not env.bool("LOCAL_DEV") else None,
        )
        return response


class AddWatchListView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user = request.user
        dataset_name: str = request.data.get("dataset", "")  # type: ignore
        dataset = dataset_metadata.get_metadata_from_external_name(dataset_name)

        if dataset is None:
            return Response({"message": "Dataset not found"}, status=404)

        WatchList.objects.get_or_create(user=user, dataset=dataset)

        return Response({"message": "Added to watchlist"})


class DeleteWatchListView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user = request.user
        dataset_name: str = request.data.get("dataset", "")  # type: ignore
        dataset = dataset_metadata.get_metadata_from_external_name(dataset_name)

        if dataset is None:
            return Response({"message": "Dataset not found"}, status=404)

        try:
            WatchList.objects.get(user=user, dataset=dataset).delete()
        except WatchList.DoesNotExist:
            return Response(
                {"message": "Dataset not found in user's watchlist"}, status=404
            )

        return Response({"message": "Deleted from watchlist"})
