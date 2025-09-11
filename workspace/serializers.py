from rest_framework import serializers
from .models import Workspace, PageImage, ProjectStatus


class WorkspaceSerializer(serializers.ModelSerializer):
    # Human-friendly labels for choices
    status_label = serializers.CharField(source="get_status_display", read_only=True)
    pipeline_state_label = serializers.CharField(
        source="get_pipeline_state_display", read_only=True
    )
    pipeline_step_label = serializers.CharField(
        source="get_pipeline_step_display", read_only=True
    )
    project_status_label = serializers.CharField(
        source="get_project_status_display", read_only=True
    )

    # Derived fields
    is_ready = serializers.BooleanField(read_only=True)
    page_counts = serializers.SerializerMethodField()
    processing_counts = serializers.SerializerMethodField()
    summary_status = serializers.SerializerMethodField()  # one badge to rule them all

    default_scale_ratio = serializers.DecimalField(
        max_digits=12,
        decimal_places=6,
        allow_null=True,
        coerce_to_string=False,
    )

    class Meta:
        model = Workspace
        fields = [
            "id",
            "name",
            # legacy status (keep for backward compatibility)
            "status",
            "status_label",
            # pipeline status
            "pipeline_state",
            "pipeline_state_label",
            "pipeline_step",
            "pipeline_step_label",
            "pipeline_progress",
            # project readiness status
            "project_status",
            "project_status_label",
            "is_ready",
            # helpful extras
            "page_counts",
            "processing_counts",
            "summary_status",
            "created_at",
            "updated_at",
            "default_scale_ratio",
            "default_scale_unit",
        ]
        read_only_fields = [
            "created_at",
            "updated_at",
            "is_ready",
            "page_counts",
            "processing_counts",
            "summary_status",
            "status_label",
            "pipeline_state_label",
            "pipeline_step_label",
            "project_status_label",
        ]

    def get_page_counts(self, obj: Workspace):
        qs = obj.pages.all()  # tip: prefetch in your view to avoid N+1
        total = qs.count()
        scaled = qs.filter(scale_ratio__isnull=False, scale_unit__isnull=False).count()
        return {"total": total, "scaled": scaled, "unscaled": total - scaled}

    def get_processing_counts(self, obj: Workspace):
        from .models import ExtractStatus
        qs = obj.pages.all()  # tip: prefetch in your view to avoid N+1
        total = qs.count()
        queued = qs.filter(extract_status=ExtractStatus.QUEUED).count()
        processing = qs.filter(extract_status=ExtractStatus.PROCESSING).count()
        return {"total": total, "queued": queued, "processing": processing}

    def get_summary_status(self, obj: Workspace):
        """
        Collapses multiple fields into a single status for your UI badge.
        Priority: pipeline running/failed > project readiness.
        """
        if obj.pipeline_state == "running":
            return {
                "code": "processing",
                "label": "Processing",
                "progress": obj.pipeline_progress,
            }
        if obj.pipeline_state in {"failed", "canceled"}:
            return {
                "code": "failed",
                "label": "Failed",
                "progress": obj.pipeline_progress,
            }

        mapping = {
            ProjectStatus.READY: ("ready", "Ready"),
            ProjectStatus.SCALED_PARTIAL: ("scaled_partial", "Scaled (Partial)"),
            ProjectStatus.SCALING_PENDING: ("scaling_pending", "Scaling Pending"),
            ProjectStatus.INCOMPLETE_SETUP: ("incomplete_setup", "Incomplete Setup"),
        }
        code, label = mapping.get(obj.project_status, ("unknown", "Unknown"))
        return {"code": code, "label": label, "progress": obj.pipeline_progress}


class PageImageSerializer(serializers.ModelSerializer):

    scale_ratio = serializers.DecimalField(
        max_digits=12,
        decimal_places=6,
        allow_null=True,
        coerce_to_string=False,
    )

    scale_units_per_px = serializers.DecimalField(
        max_digits=18, decimal_places=9, allow_null=True, coerce_to_string=False
    )
    scale_bar_crop_url = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = PageImage
        fields = [
            "id",
            "page_number",
            "width",
            "height",
            "scale_ratio",
            "scale_unit",
            "scale_units_per_px",
            "scale_bar_crop_path",
            "scale_bar_crop_url",
            "scale_bar_line_coords",
            "analyze_region",
            "dpi",
            "extract_status",
            "segmentation_choice",
        ]

    def get_scale_bar_crop_url(self, obj):
        # build from storage or MEDIA_URL; wrap with request.build_absolute_uri if you want absolute URLs
        from django.core.files.storage import default_storage
        from urllib.parse import urljoin
        from django.conf import settings

        name = obj.scale_bar_crop_path or ""
        if not name:
            return None
        try:
            return default_storage.url(name)
        except Exception:
            media_url = getattr(settings, "MEDIA_URL", "/media/")
            if not media_url.endswith("/"):
                media_url += "/"
            return urljoin(media_url, name.lstrip("/"))
