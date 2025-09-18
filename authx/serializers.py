# authx/serializers.py
import re
# from django.core.exceptions import ValidationError as DjangoValidationError
# from django.core.validators import validate_email
from rest_framework import serializers

# PHONE_MIN, PHONE_MAX = 10, 15  # digits


class SendCodeIn(serializers.Serializer):
    user_login = serializers.CharField()
    medium = serializers.ChoiceField(choices=[("EMAIL", "EMAIL"), ("SMS", "SMS")])

    def validate(self, attrs):
        medium = attrs.get("medium")
        value = (attrs.get("user_login") or "").strip()

        if medium == "EMAIL":
            # try:
            #     validate_email(value)
            # except DjangoValidationError:
            #     raise serializers.ValidationError(
            #         {"user_login": "Enter a valid email address."}
            #     )
            attrs["user_login"] = value.lower()

        elif medium == "SMS":
            # Remove phone validation - just normalize to digits
            digits = re.sub(r"\D", "", value)
            # if not (PHONE_MIN <= len(digits) <= PHONE_MAX):
            #     raise serializers.ValidationError(
            #         {"user_login": "Enter a valid phone number (10-15 digits)."}
            #     )
            attrs["user_login"] = f"{digits}"

        return attrs


class LoginIn(serializers.Serializer):
    user_login = serializers.CharField()  # was EmailField
    user_pwd = serializers.CharField(trim_whitespace=False)


class LoginOut(serializers.Serializer):
    access = serializers.CharField()
    user = serializers.DictField()
