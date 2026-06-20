import csv
from pathlib import Path

from src.llm import classify_text


def main() -> None:
    path = Path(__file__).with_name("labeled_categories.csv")
    total = correct = format_ok = 0
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            total += 1
            output, *_ = classify_text(row["text"])
            if output.canonical_category:
                format_ok += 1
            if output.canonical_category == row["expected_category"]:
                correct += 1
            print(f"{row['expected_category']}: predicted={output.canonical_category} confidence={output.confidence}")
    accuracy = correct / total if total else 0
    compliance = format_ok / total if total else 0
    print(f"accuracy={accuracy:.2%} format_compliance={compliance:.2%} examples={total}")


if __name__ == "__main__":
    main()
