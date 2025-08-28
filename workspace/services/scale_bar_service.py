from dataclasses import dataclass
from typing import Dict, Tuple, Optional
import numpy as np
import cv2
from PIL import Image
import io

# ---- import your original class/enums (put your class in this module or adjust the import)
# from yourapp.services.scale_bar_processor import ScaleBarProcessor, LineStatus
# For clarity here, I'll assume they're in the same folder:
from .scale_bar_processor import ScaleBarProcessor, LineStatus


@dataclass
class ScaleRequest:
    legend_total_length: int = 100
    min_line_length: int = 50
    max_line_gap: int = 10
    debug: bool = False


class ScaleBarService:
    """
    Thin wrapper around ScaleBarProcessor that:
     - accepts a PIL.Image
     - does safe color conversion for OpenCV
     - returns JSON-safe primitives
     - can optionally draw a debug overlay
    """

    @staticmethod
    def analyze_pil(
        pil_img: Image.Image,
        req: ScaleRequest,
    ) -> Dict:
        # PIL RGB -> OpenCV BGR
        arr_rgb = np.array(pil_img.convert("RGB"))  # (H,W,3) RGB
        arr_bgr = cv2.cvtColor(arr_rgb, cv2.COLOR_RGB2BGR)

        proc = ScaleBarProcessor(
            units="mm",
            legend_total_length=req.legend_total_length,
            min_line_length=req.min_line_length,
            max_line_gap=req.max_line_gap,
            debug=req.debug,
        )
        out = proc.process(arr_bgr)

        status = out.get("status")
        status_str = status.value if isinstance(status, LineStatus) else str(status)

        coords = out.get("longest_line_coords")
        if isinstance(coords, (list, tuple)) and len(coords) == 4:
            coords_payload = {
                "x1": int(coords[0]), "y1": int(coords[1]),
                "x2": int(coords[2]), "y2": int(coords[3]),
            }
        elif isinstance(coords, dict) and {"x1","y1","x2","y2"} <= coords.keys():
            # If the processor ever returns a dict already, pass through (normalized to int)
            coords_payload = {
                "x1": int(coords["x1"]), "y1": int(coords["y1"]),
                "x2": int(coords["x2"]), "y2": int(coords["y2"]),
            }
        else:
            coords_payload = None

        resp = {
            "status": status_str,
            "legend_total_length": req.legend_total_length,
            "pixels_per_unit": float(out.get("pixels_per_unit", 0) or 0),
            "units_per_pixel": float(out.get("units_per_pixel", 0) or 0),
            "longest_line_length_px": float(out.get("longest_line_length_px", 0) or 0),
            "longest_line_coords": coords_payload,  # <-- object with x1,y1,x2,y2
        }
        return resp

    @staticmethod
    def draw_overlay_png(
        pil_img: Image.Image,
        longest_line_coords: Optional[Tuple[int, int, int, int]],
        thickness: int = 3,
    ) -> Optional[bytes]:
        """
        Returns a PNG bytes overlay with the detected line drawn (if present).
        """
        if not longest_line_coords:
            return None

        arr_rgb = np.array(pil_img.convert("RGB"))
        arr_bgr = cv2.cvtColor(arr_rgb, cv2.COLOR_RGB2BGR)

        x1, y1, x2, y2 = map(int, longest_line_coords)
        # Draw a line in default OpenCV color (no explicit color constraints here)
        cv2.line(arr_bgr, (x1, y1), (x2, y2), color=(255, 255, 255), thickness=thickness)

        # Back to PNG bytes
        arr_rgb2 = cv2.cvtColor(arr_bgr, cv2.COLOR_BGR2RGB)
        out_img = Image.fromarray(arr_rgb2)
        buf = io.BytesIO()
        out_img.save(buf, format="PNG")
        return buf.getvalue()
