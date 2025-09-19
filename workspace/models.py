# models.py
import os
import uuid
from django.db import models
from django.conf import settings
from django.db.models.signals import post_delete
from django.dispatch import receiver

# ---------- Soft delete ----------


class SoftDeleteQuerySet(models.QuerySet):
    def delete(self):  # soft delete
        return super().update(soft_deleted=True)

    def hard_delete(self):
        return super().delete()

    def alive(self):
        return self.filter(soft_deleted=False)

    def dead(self):
        return self.filter(soft_deleted=True)


class SoftDeleteManager(models.Manager):
    def get_queryset(self):
        return SoftDeleteQuerySet(self.model, using=self._db).filter(soft_deleted=False)


# ---------- Enums ----------


class PipelineState(models.TextChoices):
    IDLE = "idle", "Idle"  # not started
    RUNNING = "running", "Running"
    SUCCEEDED = "succeeded", "Succeeded"
    FAILED = "failed", "Failed"
    CANCELED = "canceled", "Canceled"


class PipelineStep(models.TextChoices):
    QUEUED = "queued", "Queued"
    LOAD_PDF = "load_pdf", "Load PDF"
    RENDER_PAGES = "render_pages", "Render Pages to Images"
    TILE_PAGES = "tile_pages", "Divide Pages into Tiles"
    EXTRACT_POLYGONS = "extract_polygons", "Extract Polygons"
    POSTPROCESS = "postprocess", "Postprocess/Index"
    FINISHED = "finished", "Finished"


class ProjectStatus(models.TextChoices):
    INCOMPLETE_SETUP = "incomplete_setup", "Incomplete Setup"  # default
    SCALING_PENDING = "scaling_pending", "Scaling Pending"
    SCALED_PARTIAL = "scaled_partial", "Scaled (Partial)"
    READY = "ready", "Ready"


class ScaleUnit(models.TextChoices):
    INCH = "in", "Inch"
    FOOT = "ft", "Foot"
    CM = "cm", "Centimeter"
    MM = "mm", "Milimeter"
    M = "m", "Meter"
    YD = "yd", "Yard"

class SyncStatus(models.TextChoices):
    PENDING    = "pending", "Pending"
    PROCESSING = "processing", "Processing"
    FAILED     = "failed", "Failed"
    SUCCESS    = "success", "Success"

class ExtractStatus(models.TextChoices):
    NONE       = "none", "None"
    QUEUED     = "queued", "Queued"
    PROCESSING = "processing", "Processing"
    FAILED     = "failed", "Failed"
    FINISHED   = "finished", "Finished"
    CANCELED   = "canceled", "Canceled"

class SegmentationChoice(models.TextChoices):
    NONE       = "none", "None"
    GENERIC    = "generic", "Generic"
    CUSTOM     = "contoured", "Contoured"

# ---------- Workspace ----------


class Workspace(models.Model):
    # Keep your original pipeline/status if you still need it
    LEGACY_STATUS_CHOICES = [
        ("pending", "Pending"),
        ("processing", "Processing"),
        ("ready", "Ready"),
        ("failed", "Failed"),
    ]

    objects = SoftDeleteManager()
    all_objects = SoftDeleteQuerySet.as_manager()

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="workspaces",
        db_index=True,
    )
    name = models.CharField(max_length=255)
    uploaded_pdf = models.FileField(upload_to="pdfs/")

    # (Optional legacy) keep if other code depends on it:
    status = models.CharField(
        max_length=20, choices=LEGACY_STATUS_CHOICES, default="pending", db_index=True
    )

    pipeline_state = models.CharField(
        max_length=16,
        choices=PipelineState.choices,
        default=PipelineState.IDLE,
        db_index=True,
    )
    pipeline_step = models.CharField(
        max_length=32,
        choices=PipelineStep.choices,
        default=PipelineStep.QUEUED,
        db_index=True,
    )
    pipeline_progress = models.PositiveSmallIntegerField(default=0)  # 0..100

    project_status = models.CharField(
        max_length=20,
        choices=ProjectStatus.choices,
        default=ProjectStatus.INCOMPLETE_SETUP,
        db_index=True,
    )

    default_scale_ratio = models.DecimalField(
        max_digits=12, decimal_places=6, null=True, blank=True
    )
    default_scale_unit = models.CharField(
        max_length=8, choices=ScaleUnit.choices, null=True, blank=True
    )

    soft_deleted = models.BooleanField(default=False, db_index=True)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    updated_at = models.DateTimeField(auto_now=True)
    synced_at  = models.DateTimeField(null=True, blank=True)

    sync_id = models.IntegerField(null=True, blank=True)

    sync_status = models.CharField(
        max_length=16,
        choices=SyncStatus.choices,
        default=SyncStatus.PENDING,
        db_index=True,
    )

    auto_extract_on_upload = models.BooleanField(default=False)

    class Meta:
        indexes = [
            models.Index(fields=["soft_deleted", "created_at"]),
            models.Index(fields=["user", "soft_deleted", "created_at"]),
            models.Index(fields=["pipeline_state", "pipeline_step"]),
            models.Index(fields=["project_status", "soft_deleted"]),
        ]

    def __str__(self):
        return f"{self.name} ({self.pipeline_state}:{self.pipeline_step})"

    # soft-delete override
    def delete(self, using=None, keep_parents=False):
        self.soft_deleted = True
        self.save(update_fields=["soft_deleted"])

    def hard_delete(self, using=None, keep_parents=False):
        """
        Permanently delete the workspace. Also deletes its uploaded PDF file and
        lets PageImage clean up its image files via its own delete override/signal.
        """
        # Delete children explicitly to trigger their file cleanup
        for page in self.pages.all():
            page.delete()
        # Delete own PDF file
        if self.uploaded_pdf and self.uploaded_pdf.name:
            self.uploaded_pdf.delete(save=False)
        # Now remove the DB rows
        super().delete(using=using, keep_parents=keep_parents)

    @property
    def is_ready(self) -> bool:
        # Ready if project_status == READY (denormalized), or derive from pages below
        return self.project_status == ProjectStatus.READY

    def recompute_project_status(self):
        """
        Rule of thumb:
          - If ANY page has no scale set -> scaling_pending
          - If SOME pages have scale and some don't -> scaled_partial
          - If ALL pages have scale -> ready
        """
        pages = list(self.pages.only("scale_ratio", "scale_unit").all())
        if not pages:
            self.project_status = ProjectStatus.INCOMPLETE_SETUP
        else:
            has_all = all(p.scale_ratio is not None and p.scale_unit for p in pages)
            has_any = any(p.scale_ratio is not None and p.scale_unit for p in pages)
            if has_all:
                self.project_status = ProjectStatus.READY
            elif has_any:
                self.project_status = ProjectStatus.SCALED_PARTIAL
            else:
                self.project_status = ProjectStatus.SCALING_PENDING
        self.save(update_fields=["project_status"])

    @property
    def needs_sync(self) -> bool:
        # True if never synced, or changes after last sync
        return self.synced_at is None or (self.updated_at and self.synced_at and self.updated_at > self.synced_at)

def fullpage_upload_path(instance, filename):
    ext = os.path.splitext(filename)[1]
    unique_name = f"{uuid.uuid4()}{ext}"
    return f"fullpages/workspace_{instance.workspace_id}/{unique_name}"


class PageImage(models.Model):
    workspace = models.ForeignKey(
        Workspace, on_delete=models.CASCADE, related_name="pages", db_index=True
    )
    page_number = models.PositiveIntegerField(db_index=True)
    image = models.ImageField(upload_to=fullpage_upload_path)
    width = models.IntegerField()
    height = models.IntegerField()

    updated_at = models.DateTimeField(auto_now=True)
    synced_at  = models.DateTimeField(null=True, blank=True)

    sync_id = models.IntegerField(null=True, blank=True)

    scale_ratio = models.DecimalField(
        max_digits=12, decimal_places=6, null=True, blank=True
    )
    scale_unit = models.CharField(
        max_length=8, choices=ScaleUnit.choices, null=True, blank=True
    )

    scale_units_per_px     = models.DecimalField(max_digits=18, decimal_places=9, null=True, blank=True)
    scale_bar_crop_path    = models.CharField(max_length=1024, null=True, blank=True)
    scale_bar_line_coords  = models.JSONField(null=True, blank=True)

    analyze_region = models.JSONField(null=True, blank=True)
    dpi = models.IntegerField(null=True, blank=True)

    extract_status = models.CharField(
        max_length=16,
        choices=ExtractStatus.choices,
        default=ExtractStatus.NONE,
        db_index=True,
    )

    segmentation_choice = models.CharField(
        max_length=16,
        choices=SegmentationChoice.choices,
        default=SegmentationChoice.NONE,
        db_index=True,
    )

    task_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Celery task ID for tracking and cancellation"
    )


    class Meta:
        unique_together = (("workspace", "page_number"),)
        indexes = [
            models.Index(fields=["workspace", "page_number"]),
            models.Index(fields=["workspace", "scale_ratio", "scale_unit"]),
        ]

    def delete(self, using=None, keep_parents=False):
        if self.image and self.image.name:
            self.image.delete(save=False)
        return super().delete(using=using, keep_parents=keep_parents)

    @property
    def image_shape(self):
        return [self.height, self.width, 3]

    @property
    def needs_sync(self) -> bool:
        # True if never synced, or changes after last sync
        return self.synced_at is None or (self.updated_at and self.synced_at and self.updated_at > self.synced_at)

    def set_task(self, task):
        """Store Celery task instance and its ID in the database"""
        self.task_id = task.id
        self.save(update_fields=['task_id'])
        return task

    def get_task(self):
        """Get Celery task instance from stored task ID"""
        if not self.task_id:
            return None
        try:
            from celery import current_app
            return current_app.AsyncResult(self.task_id)
        except Exception:
            return None

    def cancel_task(self):
        """Cancel the stored Celery task"""
        task = self.get_task()
        if task:
            try:
                from celery import current_app
                current_app.control.revoke(self.task_id, terminate=True)
                print(f"[CANCEL] Revoked Celery task {self.task_id} for page {self.id}")
                return True
            except Exception as e:
                print(f"[CANCEL] Error revoking task {self.task_id}: {e}")
                return False
        return False

    def clear_task(self):
        """Clear the stored task ID"""
        self.task_id = None
        self.save(update_fields=['task_id'])

class Tag(models.Model):
    workspace = models.ForeignKey(
        Workspace,
        on_delete=models.CASCADE,
        related_name="tags",
        db_index=True
    )
    label = models.CharField(max_length=100, db_index=True)
    color = models.CharField(max_length=7, help_text="Hex color like #RRGGBB")
    sync_id = models.IntegerField(null=True, blank=True)
    synced_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = (("workspace", "label"),)
        indexes = [
            models.Index(fields=["workspace", "label"], name="workspace_tag_ws_label_idx"),
        ]

    def __str__(self):
        return f"{self.label} ({self.color})"


@receiver(post_delete, sender=PageImage)
def _cleanup_pageimage_file(sender, instance, **kwargs):
    if instance.image and instance.image.name:
        try:
            instance.image.delete(save=False)
        except Exception:
            pass  # ignore storage errors

@receiver(post_delete, sender=Workspace)
def _cleanup_workspace_pdf(sender, instance, **kwargs):
    if instance.uploaded_pdf and instance.uploaded_pdf.name:
        try:
            instance.uploaded_pdf.delete(save=False)
        except Exception:
            pass
