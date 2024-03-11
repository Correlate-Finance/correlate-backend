from django.contrib import admin

from .models import User
from .models import WatchList

admin.site.register(User)
admin.site.register(WatchList)
