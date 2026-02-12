from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.styles import ParagraphStyle
from datetime import datetime
import json
import os

# --------------------------------------------------
# Paths
# --------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INTERACTIONS_REPORT_DIR = os.path.join(BASE_DIR, "interactions")
PRODUCTS_REPORT_DIR = os.path.join(BASE_DIR, "products")

# Output PDF will be saved directly inside p005_data_quality_reports/
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_PDF = os.path.join(BASE_DIR, f"DQ_Report_{TIMESTAMP}.pdf")


# --------------------------------------------------
# Utility functions
# --------------------------------------------------

def get_latest_file(folder):
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".json")]
    if not files:
        raise Exception(f"No JSON reports found in {folder}")
    return max(files, key=os.path.getmtime)


def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


# --------------------------------------------------
# PDF Builder
# --------------------------------------------------

def build_pdf(interactions_report, products_report):
    doc = SimpleDocTemplate(OUTPUT_PDF, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []

    # Title
    title_style = ParagraphStyle(
        name="TitleStyle",
        fontSize=18,
        leading=22,
        alignment=TA_CENTER
    )

    story.append(Paragraph("Data Quality Report – DM4ML Assignment", title_style))
    story.append(Spacer(1, 20))
    story.append(Paragraph(
        f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        styles["Normal"]
    ))
    story.append(Spacer(1, 30))

    # ------------------ INTERACTIONS ------------------

    story.append(Paragraph("1. Interactions Dataset", styles["Heading2"]))
    story.append(Spacer(1, 10))

    story.append(Paragraph(f"<b>Source File:</b> {interactions_report['file_used']}", styles["Normal"]))
    story.append(Paragraph(f"<b>Total Records:</b> {interactions_report['total_records']}", styles["Normal"]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("<b>Null Values:</b>", styles["Normal"]))
    for col, cnt in interactions_report["null_counts"].items():
        story.append(Paragraph(f"{col}: {cnt}", styles["Normal"]))

    story.append(Spacer(1, 10))
    story.append(Paragraph("<b>Validation Results:</b>", styles["Normal"]))
    for k, v in interactions_report["validation_results"].items():
        story.append(Paragraph(f"{k}: {v}", styles["Normal"]))

    story.append(Spacer(1, 30))

    # ------------------ PRODUCTS ------------------

    story.append(Paragraph("2. Products Dataset", styles["Heading2"]))
    story.append(Spacer(1, 10))

    story.append(Paragraph(f"<b>Source File:</b> {products_report['file_used']}", styles["Normal"]))
    story.append(Paragraph(f"<b>Total Records:</b> {products_report['total_records']}", styles["Normal"]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("<b>Validation Results:</b>", styles["Normal"]))
    for k, v in products_report["validation_results"].items():
        story.append(Paragraph(f"{k}: {v}", styles["Normal"]))

    story.append(Spacer(1, 30))

    # ------------------ CONCLUSION ------------------

    story.append(Paragraph("3. Conclusion", styles["Heading2"]))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "The data quality checks were successfully executed on both the interaction "
        "and product datasets. Each PDF report is automatically versioned using a "
        "timestamp, ensuring full auditability and traceability of data quality "
        "across pipeline runs.",
        styles["Normal"]
    ))

    doc.build(story)


# --------------------------------------------------
# Main
# --------------------------------------------------

if __name__ == "__main__":
    print("Generating Data Quality PDF Report...")

    latest_interactions_json = get_latest_file(INTERACTIONS_REPORT_DIR)
    latest_products_json = get_latest_file(PRODUCTS_REPORT_DIR)

    interactions_report = load_json(latest_interactions_json)
    products_report = load_json(latest_products_json)

    build_pdf(interactions_report, products_report)

    print(f"Data Quality PDF generated successfully:")
    print(OUTPUT_PDF)
