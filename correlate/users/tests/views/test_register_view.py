from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient
from users.models import Allowlist


class RegisterViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.register_url = reverse("register")  # Replace with your actual URL name
        Allowlist.objects.create(email="allowed@example.com")

    def test_register_with_allowed_email(self):
        data = {
            "email": "allowed@example.com",
            "password": "testpassword",
            "name": "name",
        }
        response = self.client.post(self.register_url, data)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_register_with_not_allowed_email(self):
        data = {
            "email": "notallowed@example.com",
            "password": "testpassword",
            "name": "name",
        }
        response = self.client.post(self.register_url, data)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
