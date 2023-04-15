def clear_text(text: str) -> str:
    strings = text.split("\n")
    target_text = "\n".join([string.strip() for string in strings])

    return target_text
