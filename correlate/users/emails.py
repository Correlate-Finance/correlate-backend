from django.core.mail import send_mail
from django.conf import settings
from .models import User
import random

def send_otp_via_email(email):
    subject = 'Your OTP for password reset'
    otp = random.randint(100000, 999999)
    message = f'Your OTP is {otp}'
    email_from = settings.EMAIL_HOST
    send_mail(subject, message, email_from, [email])
    user = User.objects.get(email=email)
    user.otp = otp
    user.save()
