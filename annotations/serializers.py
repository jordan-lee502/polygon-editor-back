from rest_framework import serializers
from annotations.models import Polygon, PolygonTag
from workspace.serializers import TagSerializer

class PolygonSerializer(serializers.ModelSerializer):
    page_number = serializers.IntegerField(source='page.page_number')
    tags = serializers.SerializerMethodField()

    class Meta:
        model = Polygon
        fields = ['id', 'polygon_id', 'page_number', 'vertices', 'visible', 'name', 'tags']

    def get_tags(self, obj):
        """Get all tags associated with this polygon through PolygonTag table"""

        # Get PolygonTag relations for this polygon
        polygon_tags = PolygonTag.objects.filter(polygon=obj).select_related('tag')
        tags = [pt.tag for pt in polygon_tags]
        return TagSerializer(tags, many=True).data
