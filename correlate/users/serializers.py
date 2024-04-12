from rest_framework import serializers
from .models import User


class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ["id", "name", "email", "password"]
        extra_kwargs = {"password": {"write_only": True}}

    def validate_email(self, value: str):
        return value.lower()

    def create(self, validated_data):
        password = validated_data.pop("password", None)

        instance = self.Meta.model(**validated_data)
        if password is not None:
            instance.set_password(password)

        instance.save()
        return instance


class UserAuthenticationSerializer(serializers.Serializer):
    email = serializers.CharField(required=True, allow_blank=False, max_length=255)
    password = serializers.CharField(required=True, allow_blank=False, max_length=255)

    def validate_email(self, value: str):
        return value.lower()


class IndexSerializer(serializers.Serializer):
    name = serializers.CharField(required=True, allow_blank=False, max_length=255)
    datasets = serializers.ListField(child=serializers.DictField())

    def validate_name(self, value: str):
        return value.lower()
