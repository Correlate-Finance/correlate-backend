from rest_framework import serializers
from .models import User, IndexDataset, Index


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


class IndexDatasetSerializer(serializers.ModelSerializer):
    class Meta:
        model = IndexDataset
        fields = ['dataset', 'weight']


class IndexSerializer(serializers.ModelSerializer):
    index_datasets = IndexDatasetSerializer(many=True, read_only=True)

    class Meta:
        model = Index
        fields = ['id', 'name', 'user', 'aggregation_period', 'correlation_metric', 'created_at', 'datasets']

    def create(self, validated_data):
        index_datasets_data = validated_data.pop('index_datasets')
        index = Index.objects.create(**validated_data)
        for index_dataset_data in index_datasets_data:
            IndexDataset.objects.create(index=index, **index_dataset_data)
        return index
