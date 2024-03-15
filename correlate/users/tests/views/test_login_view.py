from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from rest_framework.authtoken.models import Token
from users.models import User, Allowlist


class LoginViewTestCase(APITestCase):
    def setUp(self):
        register_url = reverse("register")

        # Register our user
        Allowlist.objects.create(email="allowed@example.com")
        data = {
            "email": "allowed@example.com",
            "password": "testpassword",
            "name": "name",
        }
        self.client.post(register_url, data)

    def test_successful_login(self):
        url = reverse("login")  # Adjust with your login URL's name
        data = {"email": "allowed@example.com", "password": "testpassword"}
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("token", response.json())

    def test_login_incorrect_password(self):
        url = reverse("login")
        data = {"email": "logintest@example.com", "password": "wrongpassword"}
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_login_nonexistent_user(self):
        url = reverse("login")
        data = {"email": "nonexistent@example.com", "password": "irrelevantPassword"}
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
