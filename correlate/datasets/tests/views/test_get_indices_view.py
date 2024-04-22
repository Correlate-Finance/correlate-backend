from django.test import TestCase
from rest_framework.test import APIClient
from rest_framework import status
from users.models import User
from datasets.models import Index, IndexDataset, DatasetMetadata
from django.urls import reverse
from rest_framework.authtoken.models import Token


class GetIndicesViewTests(TestCase):
    def setUp(self):
        # Create a user and obtain a token
        self.user = User.objects.create(email="testuser", password="12345")
        self.token, _ = Token.objects.get_or_create(user=self.user)
        self.client = APIClient()

    def test_get_indices_authenticated(self):
        # Create some indices for the user
        index = Index.objects.create(name="Index 1", user=self.user)

        dm1 = DatasetMetadata.objects.create(
            internal_name="table_name",
            external_name="Title",
            source="Source",
            description="Description",
        )

        dm2 = DatasetMetadata.objects.create(
            internal_name="table_name2",
            external_name="Title",
            source="Source",
            description="Description",
        )

        IndexDataset.objects.create(index=index, weight=0.5, dataset=dm1)
        IndexDataset.objects.create(index=index, weight=0.5, dataset=dm2)

        index1 = Index.objects.create(name="Index ", user=self.user)
        IndexDataset.objects.create(index=index1, weight=0.5, dataset=dm1)
        IndexDataset.objects.create(index=index1, weight=0.5, dataset=dm2)

        # Set the token in the request headers
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")
        response = self.client.get(reverse("get-indices"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)  # Check if all indices are returned
        self.assertEqual(
            response.data[0]["name"], "Index 1"
        )  # Check if the first index is correct
        self.assertEqual(
            response.data[0]["index_datasets"][0]["dataset"]["internal_name"],
            "table_name",
        )
        self.assertEqual(
            response.data[0]["index_datasets"][1]["dataset"]["internal_name"],
            "table_name2",
        )

    def test_get_indices_unauthenticated(self):
        # No token provided, simulating unauthenticated state
        response = self.client.get(reverse("get-indices"))

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
