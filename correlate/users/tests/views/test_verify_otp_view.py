from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient
from users.models import User

class VerifyOTPViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.verify_otp_url = reverse("verify-otp")  # Replace with your actual URL name

    def test_verify_otp_with_correct_data(self):
        user = User.objects.create(email="existing_user@example.com", otp="123456")
        response = self.client.post(self.verify_otp_url, {"email": user.email, "otp": user.otp})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["message"], "OTP is correct")

    def test_verify_otp_with_incorrect_otp(self):
        user = User.objects.create(email="existing_user@example.com", otp="123456")
        response = self.client.post(self.verify_otp_url, {"email": user.email, "otp": "567890"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["message"], "OTP is incorrect")

    def test_verify_otp_with_non_existing_user(self):
        response = self.client.post(self.verify_otp_url, {"email": "non_existing_user@example.com", "otp": "1234"})
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(response.data["message"], "User not found")
