import math
import enum
import numpy as np
import cv2


class LineStatus(enum.Enum):
    """Enumeration for line processing status."""

    NO_LINES_FOUND = "no_lines_found"
    NO_LONGEST_LINE_FOUND = "no_longest_line_found"
    SUCCESS = "success"
    ERROR = "error"


class ScaleBarProcessor:
    """
    A class to process scale bars in images.
    """

    def __init__(
        self,
        units="mm",
        legend_total_length=100,
        min_line_length=50,
        max_line_gap=10,
        debug=False,
    ):
        """Initialize the ScaleBarProcessor."""
        self.units = units
        self.legend_total_length = legend_total_length
        self.min_line_length = min_line_length
        self.max_line_gap = max_line_gap
        self.debug = debug

    def process(self, image: np.ndarray):
        """Process the image to find and measure the scale bar."""
        if self.debug:
            print(f"Processing image of shape: {image.shape}")

        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        longest_line_info = self._find_longest_horizontal_line(gray)

        if longest_line_info["status"] != LineStatus.SUCCESS:
            if self.debug:
                print(
                    f"Error finding longest horizontal line: {longest_line_info['status']}"
                )
            return {
                "status": LineStatus.ERROR,
                "pixels_per_unit": 0,
                "units_per_pixel": 0,
                "longest_line_length_px": [],
                "longest_line_coords": [],
            }

        pixels_per_unit = longest_line_info["longest_line"] / self.legend_total_length
        units_per_pixel = self.legend_total_length / longest_line_info["longest_line"]

        return {
            "status": LineStatus.SUCCESS,
            "pixels_per_unit": pixels_per_unit,
            "units_per_pixel": units_per_pixel,
            "longest_line_length_px": longest_line_info["longest_line"],
            "longest_line_coords": longest_line_info.get("longest_line_coords"),
        }

    def _find_longest_horizontal_line(self, scale_image: np.ndarray):
        edges = cv2.Canny(scale_image, 50, 150, apertureSize=3)
        lines = cv2.HoughLinesP(
            edges,
            1,
            np.pi / 180,
            threshold=100,
            minLineLength=self.min_line_length,
            maxLineGap=self.max_line_gap,
        )

        if lines is None:
            if self.debug:
                print("No lines found")
            return {"lines": [], "status": LineStatus.NO_LINES_FOUND}

        longest_line = 0
        longest_line_coords = None

        for line in lines:
            x1, y1, x2, y2 = line[0]
            angle = math.atan2(abs(y2 - y1), abs(x2 - x1)) * 180 / math.pi

            if angle < 10:
                length = np.linalg.norm(np.array([x2 - x1, y2 - y1]))
                if length > longest_line:
                    longest_line = length
                    longest_line_coords = (x1, y1, x2, y2)

        if self.debug:
            print(f"Longest horizontal line length: {longest_line}")
            if longest_line_coords:
                print(f"Longest line coordinates: {longest_line_coords}")

        if longest_line == 0:
            return {"lines": [], "status": LineStatus.NO_LONGEST_LINE_FOUND}

        return {
            "lines": lines,
            "status": LineStatus.SUCCESS,
            "longest_line": longest_line,
            "longest_line_coords": longest_line_coords,
        }