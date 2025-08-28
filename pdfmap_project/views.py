from django.http import FileResponse
import os
from django.conf import settings

def index(request):
    index_path = os.path.join(settings.BASE_DIR, 'pdfmap_project', 'static', 'index.html')
    return FileResponse(open(index_path, 'rb'))
