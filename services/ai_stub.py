from models import Product


def generate_description(product: Product) -> str:
    return (
        f"{product.base_name} — практичный товар для ежедневного использования. "
        f"Артикул: {product.article or 'не указан'}. "
        f"Цвет: {product.color or 'не указан'}."
    )


def generate_photo_prompt(product: Product) -> str:
    return (
        f"Сделай фото товара '{product.base_name}' на чистом белом фоне, "
        f"подчеркни цвет '{product.color or 'нейтральный'}', формат для маркетплейса."
    )


def generate_infographic_prompt(product: Product) -> str:
    return (
        f"Создай инфографику для '{product.base_name}' с блоками: размеры, вес, преимущества. "
        f"Добавь артикул {product.article or 'N/A'}."
    )
