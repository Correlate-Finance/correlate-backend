from django.contrib import admin

from .models import User, WatchList, Allowlist

admin.site.register(User)
admin.site.register(WatchList)
admin.site.register(Allowlist)
