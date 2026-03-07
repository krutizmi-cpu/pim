def generate_product_name(
    base_name: str | None,
    article: str | None,
    color: str | None,
    category_hint: str | None,
    max_length: int = 75,
) -> str:
    base = (base_name or "Товар").strip()
    parts = [base]

    if article:
        parts.append(f"арт. {article.strip()}")
    if color:
        parts.append(f"({color.strip()})")
    if category_hint and category_hint.lower() not in base.lower():
        parts.insert(0, category_hint.strip())

    candidate = " ".join([p for p in parts if p])
    if len(candidate) <= max_length:
        return candidate

    # Trim less important parts first.
    compact = " ".join([parts[0], parts[1]]) if len(parts) > 1 else parts[0]
    if len(compact) <= max_length:
        return compact
    return compact[: max_length - 1].rstrip() + "…"
