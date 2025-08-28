from django.contrib import admin

# Register your models here.
from django.contrib import admin
from .models import Workspace

@admin.register(Workspace)
class WorkspaceAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'status', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('name',)
