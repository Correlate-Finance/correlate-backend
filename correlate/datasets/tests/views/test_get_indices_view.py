from django.test import TestCase
from rest_framework.test import APIClient
from rest_framework import status
from users.models import User
from datasets.models import Index
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
        Index.objects.create(name="Index 1", user=self.user)
        Index.objects.create(name="Index 2", user=self.user)

        # Set the token in the request headers
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")
        response = self.client.get(reverse("get-indices"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)  # Check if all indices are returned
        self.assertEqual(response.data[0]["name"], "Index 1")  # Check if the first index is correct

    def test_get_indices_unauthenticated(self):
        # No token provided, simulating unauthenticated state
        response = self.client.get(reverse("get-indices"))

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
