from django.http import FileResponse, JsonResponse
import os
from django.conf import settings
import redis
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

def index(request):
    index_path = os.path.join(settings.BASE_DIR, 'pdfmap_project', 'static', 'index.html')
    return FileResponse(open(index_path, 'rb'))

@csrf_exempt
@require_http_methods(["GET"])
def health_redis(request):
    """
    Health check endpoint for Redis connection.
    Returns 'healthy' if Redis is accessible, 'unhealthy' otherwise.
    """
    try:
        # Get Redis URL from settings
        redis_url = getattr(settings, 'CELERY_BROKER_URL', 'redis://localhost:6379/0')

        # Create Redis connection
        r = redis.from_url(redis_url)

        # Test Redis connection with a simple ping
        r.ping()

        return JsonResponse({"status": "healthy"}, status=200)

    except Exception as e:
        return JsonResponse({
            "status": "unhealthy",
            "error": str(e)
        }, status=503)
