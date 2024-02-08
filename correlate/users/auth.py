from rest_framework.authentication import TokenAuthentication


class TokenAuthSupportCookie(TokenAuthentication):
    """
    Extend the TokenAuthentication class to support cookie based authentication
    """

    def authenticate(self, request):
        # Check if 'auth_token' is in the request cookies.
        # Give precedence to 'Authorization' header.
        if "session" in request.COOKIES and "HTTP_AUTHORIZATION" not in request.META:
            return self.authenticate_credentials(request.COOKIES.get("session"))
        return super().authenticate(request)
