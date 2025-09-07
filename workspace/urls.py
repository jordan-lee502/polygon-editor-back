# workspace/urls.py
from django.urls import path, include
from . import views

urlpatterns = [
    path("workspaces/", views.list_workspaces, name="list_workspaces"),
    path(
        "workspaces/<int:workspace_id>/pages/",
        views.workspace_pages,
        name="workspace_pages",
    ),
    path(
        "workspaces/<int:workspace_id>/polygons/",
        views.workspace_polygons,
        name="workspace_polygons",
    ),
    path(
        "workspaces/<int:workspace_id>/pages/<int:page_id>/polygons/create/",
        views.create_single_polygon,
        name="create-single-polygon",
    ),
    path(
        "workspaces/<int:workspace_id>/pages/<int:page_id>/polygons/<int:polygon_id>/delete/",
        views.delete_single_polygon,
        name="delete-single-polygon",
    ),
    path(
        "workspaces/<int:workspace_id>/pages/<int:page_id>/polygons/delete-multiple/",
        views.delete_multiple_polygons,
        name="delete-multiple-polygons",
    ),
    path(
        "workspaces/<int:workspace_id>/pages/<int:page_id>/polygons/",
        views.workspace_page_polygons,
        name="workspace-page-polygons",
    ),
    path("polygons/<int:polygon_id>/", views.update_polygon, name="update_polygon"),
    path(
        "workspaces/<int:workspace_id>/export-analysis/",
        views.export_analysis,
        name="export_analysis",
    ),
    path(
        "workspaces/<int:workspace_id>/soft-delete/",
        views.soft_delete_workspace,
        name="soft_delete_workspace",
    ),
    path(
        "workspaces/<int:workspace_id>/restore/",
        views.restore_workspace,
        name="restore_workspace",
    ),
    path(
        "workspaces/<int:workspace_id>/hard-delete/",
        views.hard_delete_workspace,
        name="hard_delete_workspace",
    ),
    path(
        "workspaces/<int:workspace_id>/scale/",
        views.patch_workspace_scale,
        name="workspace-scale",
    ),
    path("pages/<int:page_id>/scale/", views.patch_page_scale, name="page-scale"),
    path("pages/<int:page_id>/scale/analyze/", views.analyze_page_scale, name="page-scale-analyze"),
    path("uploads/", include("uploads.urls")),
]
