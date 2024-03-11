from django.db import models
from django.contrib.auth.models import AbstractUser
from datasets.models import DatasetMetadata


# Create your models here.
class User(AbstractUser):
    id: int
    name = models.CharField(max_length=255)

    email = models.EmailField(max_length=255, unique=True)
    password = models.CharField(max_length=255)
    username = None

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = []


class WatchList(models.Model):
    id = models.AutoField(primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    dataset = models.ForeignKey(DatasetMetadata, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.user.email} - {self.dataset.name}"
