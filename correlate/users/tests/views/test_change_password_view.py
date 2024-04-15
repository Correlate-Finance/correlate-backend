from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient
from users.models import User


class ChangePasswordViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.change_password_url = reverse("change-password")
        self.user = User.objects.create(email="existing_user@example.com")
        self.password = "oldpassword"
        self.user.set_password(self.password)
        self.user.save()

    def test_change_password_with_correct_data(self):
        new_password = "newpassword"
        data = {"email": self.user.email, "password": new_password}
        response = self.client.post(self.change_password_url, data)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["message"], "Password changed")
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password(new_password))

    def test_change_password_with_non_existing_user(self):
        data = {"email": "non_existing_user@example.com", "password": "newpassword"}
        response = self.client.post(self.change_password_url, data)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(response.data["message"], "User not found")

    def test_change_password_with_missing_password(self):
        data = {"email": self.user.email}
        response = self.client.post(self.change_password_url, data)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["message"], "Password is required")
