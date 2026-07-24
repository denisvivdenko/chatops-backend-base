PDF_CONTENT = b"%PDF-1.4\n%mock pdf content"


def upload_resource(client, filename: str = "report.pdf", content: bytes = PDF_CONTENT) -> str:
    response = client.post("/api/upload-resource", files={"file": (filename, content, "application/pdf")})
    return response.json()["id"]
