from rest_framework import serializers
from annotations.models import Polygon

class PolygonSerializer(serializers.ModelSerializer):
    page_number = serializers.IntegerField(source='page.page_number')

    class Meta:
        model = Polygon
        fields = ['id', 'polygon_id', 'page_number', 'vertices', 'visible']
