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
        views.create_multi_polygon,
        name="create-multi-polygon",
    ),
    path(
        "workspaces/<int:workspace_id>/pages/<int:page_id>/polygons/create-multi/",
        views.create_multi_polygon,
        name="create-multi-polygon-alt",
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
    path("workspaces/<int:workspace_id>/pages/<int:page_id>/analyze-region/", views.analyze_region, name="analyze-region"),
    path("workspaces/<int:workspace_id>/pages/<int:page_id>/status/", views.update_page_status, name="update-page-status"),
    path("workspaces/<int:workspace_id>/pages/<int:page_id>/cancel-analysis/", views.cancel_region_analysis, name="cancel-region-analysis"),
    path("projects/<int:workspace_id>/tags/", views.workspace_tags, name="workspace-tags"),
    path("projects/<int:workspace_id>/tags/<int:tag_id>/", views.workspace_tag_detail, name="workspace-tag-detail"),
    path("uploads/", include("uploads.urls")),
]
