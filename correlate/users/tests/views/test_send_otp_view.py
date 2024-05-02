from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient
from django.core import mail
from users.models import User


class SendOTPViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.send_otp_url = reverse("send-otp")  # Replace with your actual URL name

    def test_send_otp_with_existing_user(self):
        user = User.objects.create(email="existing_user@example.com")
        response = self.client.post(self.send_otp_url, {"email": user.email})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["message"], "OTP sent via email")
        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].to, [user.email])

    def test_send_otp_with_non_existing_user(self):
        response = self.client.post(
            self.send_otp_url, {"email": "non_existing_user@example.com"}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["message"], "OTP sent via email")
        self.assertEqual(len(mail.outbox), 0)
