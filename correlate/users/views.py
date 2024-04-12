from django.http import HttpResponse
from rest_framework.request import Request
from rest_framework.views import APIView
from rest_framework.authtoken.models import Token
from .serializers import UserSerializer, UserAuthenticationSerializer, IndexSerializer
from rest_framework.response import Response
from rest_framework.exceptions import AuthenticationFailed
from rest_framework.permissions import IsAuthenticated
from .models import User, WatchList, Allowlist, IndexDataset, Index
import environ
from datasets import dataset_metadata_orm
from rest_framework.status import HTTP_403_FORBIDDEN
from typing import List


env = environ.Env()
environ.Env.read_env()


class RegisterView(APIView):
    serializer_class = UserSerializer

    def post(self, request: Request) -> HttpResponse:
        serializer = UserSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        email = serializer.validated_data.get("email")  # type: ignore

        if not Allowlist.objects.filter(email=email).exists():
            return Response({"detail": "Email not allowed."}, status=HTTP_403_FORBIDDEN)

        serializer.save()
        return Response(serializer.data)


class LoginView(APIView):
    serializer_class = UserAuthenticationSerializer
    authentication_classes = []

    def post(self, request: Request) -> HttpResponse:
        serializer = UserAuthenticationSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        user = User.objects.filter(email=serializer.validated_data.get("email")).first()  # type: ignore
        if user is None:
            raise AuthenticationFailed("User not found")

        if not user.check_password(serializer.validated_data.get("password")):  # type: ignore
            raise AuthenticationFailed("Password is incorrect")

        token, _ = Token.objects.get_or_create(user=user)
        return Response(data={"token": token.key})


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


class WatchlistedView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        user = request.user
        dataset_names: List[str] = request.data.get("datasets", [])  # type: ignore

        watchlists = WatchList.objects.filter(
            user=user, dataset__external_name__in=dataset_names
        ).prefetch_related("dataset")
        watchlist_map = {w.dataset.external_name: w for w in watchlists}
        result = [dataset in watchlist_map for dataset in dataset_names]
        return Response({"watchlisted": result})


class AddWatchListView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user = request.user
        dataset_name: str = request.data.get("dataset", "")  # type: ignore
        dataset = dataset_metadata_orm.get_metadata_from_external_name(dataset_name)

        if dataset is None:
            return Response({"message": "Dataset not found"}, status=404)

        WatchList.objects.get_or_create(user=user, dataset=dataset)

        return Response({"message": "Added to watchlist"})


class DeleteWatchListView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user = request.user
        dataset_name: str = request.data.get("dataset", "")  # type: ignore
        dataset = dataset_metadata_orm.get_metadata_from_external_name(dataset_name)

        if dataset is None:
            return Response({"message": "Dataset not found"}, status=404)

        try:
            WatchList.objects.get(user=user, dataset=dataset).delete()
        except WatchList.DoesNotExist:
            return Response(
                {"message": "Dataset not found in user's watchlist"}, status=404
            )

        return Response({"message": "Deleted from watchlist"})


class SaveIndexView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user = request.user
        datasets: List[dict] = request.data.get("datasets", [])  # type: ignore
        name: str = request.data.get("name", "")
        aggregation_period: str = request.data.get("aggregation_period", "")
        correlation_metric: str = request.data.get("correlation_metric", "")

        datasets_list = []
        for dataset in datasets:
            external_name = dataset.get("title", "")
            weight = dataset.get("weight", 0.0)
            dataset = dataset_metadata_orm.get_metadata_from_external_name(external_name)
            
            if dataset is None:
                return Response({"message": "Dataset not found"}, status=404)
            
            IndexDataset.objects.create(dataset=dataset, weight=weight)
            datasets_list.append(dataset)
            
        Index.objects.create(
            name=name,
            user=user,
            datasets=datasets_list,
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
        )
        return Response({"message": "Index saved"})


class GetIndexView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        user = request.user
        indices = Index.objects.filter(user=user)
        serializer = IndexSerializer(indices, many=True)
        return Response(serializer.data)
