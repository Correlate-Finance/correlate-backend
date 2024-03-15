from django.test import TestCase
from users.models import User
from users.serializers import UserSerializer


class UserSerializerTestCase(TestCase):
    def test_create_user(self):
        serializer = UserSerializer(
            data={
                "name": "John Doe",
                "email": "JOHN@EXAMPLE.COM",
                "password": "password123",
            }
        )
        self.assertTrue(serializer.is_valid())
        user: User = serializer.save()  # type: ignore
        self.assertEqual(user.email, "john@example.com")
        self.assertTrue(user.check_password("password123"))

    def test_create_user_invalid_data(self):
        serializer = UserSerializer(data={})
        self.assertFalse(serializer.is_valid())

    def test_email_lowercase(self):
        serializer = UserSerializer(
            data={
                "name": "Jane Doe",
                "email": "JANE@EXAMPLE.COM",
                "password": "password456",
            }
        )
        self.assertTrue(serializer.is_valid())
        self.assertEqual(serializer.validated_data["email"], "jane@example.com")  # type: ignore
