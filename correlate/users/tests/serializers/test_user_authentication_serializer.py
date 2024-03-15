from users.serializers import UserAuthenticationSerializer
from django.test import TestCase


class UserAuthenticationSerializerTestCase(TestCase):
    def test_validate_email_lowercase(self):
        serializer = UserAuthenticationSerializer(
            data={"email": "AnotherTestEmail@Example.com", "password": "password123"}
        )
        self.assertTrue(serializer.is_valid())
        self.assertEqual(
            serializer.validated_data.get("email"),  # type: ignore
            "anothertestemail@example.com",
        )
