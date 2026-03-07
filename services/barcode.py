def needs_registration(barcode: str | None) -> bool:
    barcode = (barcode or "").strip()
    return (not barcode) or barcode.startswith("2")
