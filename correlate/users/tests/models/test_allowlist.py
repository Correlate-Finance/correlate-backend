# tests.py in the accounts app

from django.test import TestCase
from users.models import Allowlist
from django.db import IntegrityError
from django.core.exceptions import ValidationError


class AllowlistModelTest(TestCase):
    def test_add_valid_email(self):
        email = "test@example.com"
        allowlist_entry = Allowlist.objects.create(email=email)
        self.assertEqual(allowlist_entry.email, email)

    def test_add_duplicate_email(self):
        email = "duplicate@example.com"
        Allowlist.objects.create(email=email)
        with self.assertRaises(IntegrityError):
            Allowlist.objects.create(email=email)

    def test_email_validation(self):
        invalid_email = "invalid-email"
        with self.assertRaises(ValidationError):
            Allowlist.objects.create(email=invalid_email).full_clean()
