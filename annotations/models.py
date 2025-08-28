from django.db import models
from workspace.models import Workspace, PageImage


class Polygon(models.Model):
    workspace = models.ForeignKey(
        Workspace, on_delete=models.CASCADE, related_name="polygons"
    )
    page = models.ForeignKey(
        PageImage, on_delete=models.CASCADE, related_name="polygons"
    )
    polygon_id = models.PositiveIntegerField()
    total_vertices = models.PositiveIntegerField()
    vertices = models.JSONField()  # List of [x, y] pairs

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    synced_at  = models.DateTimeField(null=True, blank=True)

    sync_id = models.IntegerField(null=True, blank=True)

    def __str__(self):
        return f"Polygon {self.polygon_id} on Page {self.page.page_number}"

    @property
    def area(self):
        if not self.vertices or not self.vertices:
            return 0
        pts = self.vertices
        n = len(pts)
        if n < 3:
            return 0
        # Shoelace formula
        return abs(
            sum(
                pts[i][0] * pts[(i + 1) % n][1] - pts[(i + 1) % n][0] * pts[i][1]
                for i in range(n)
            )
            / 2.0
        )

    @property
    def bbox(self):
        if not self.vertices or not self.vertices:
            return [0, 0, 0, 0]
        xs = [point[0] for point in self.vertices]
        ys = [point[1] for point in self.vertices]
        return [min(xs), min(ys), max(xs), max(ys)]
    
    @property
    def area_percentage(self):
        return 0

    @property
    def area_inches(self):
        if not self.area:
            return 0.0
        dpi = 100  # Or dynamically pass DPI
        pixel_area = self.area
        inch_area = pixel_area / (dpi * dpi)
        return round(inch_area, 4)
    
    @property
    def size_category(self):
        if not self.area:
            return "unknown"
        if self.area < 1000:
            return "small"
        elif self.area < 10000:
            return "medium"
        else:
            return "large"
        
    @property
    def needs_sync(self) -> bool:
        # True if never synced, or changes after last sync
        return self.synced_at is None or (self.updated_at and self.synced_at and self.updated_at > self.synced_at)
