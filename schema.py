# schema.py
# Hardcoded schema from the POC semantic model (PowerBI_POC2)
# sales = fact table | customers, products = dimension tables
#
# Relationships:
#   sales.CustomerID -> customers.CustomerID
#   sales.ProductID  -> products.ProductID

SCHEMA = {
    "tables": {
        "sales": {
            "type": "fact",
            "columns": {
                "SalesID":     "STRING",
                "CustomerID":  "STRING",
                "ProductID":   "STRING",
                "Quantity":    "INTEGER",
                "SalesDate":   "DATE",
            }
        },
        "customers": {
            "type": "dimension",
            "columns": {
                "CustomerID":   "STRING",
                "CustomerName": "STRING",
                "City":         "STRING",
                "State":        "STRING",
                "Country":      "STRING",
                "JoinDate":     "DATE",
            }
        },
        "products": {
            "type": "dimension",
            "columns": {
                "ProductID":   "STRING",
                "ProductName": "STRING",
                "Category":    "STRING",
                "Price":       "DECIMAL",
            }
        },
    },
    "relationships": [
        {
            "from_table":  "sales",
            "from_column": "CustomerID",
            "to_table":    "customers",
            "to_column":   "CustomerID",
        },
        {
            "from_table":  "sales",
            "from_column": "ProductID",
            "to_table":    "products",
            "to_column":   "ProductID",
        },
    ]
}


def get_schema_for_llm() -> str:
    """
    Returns the schema as a formatted string for the LLM system prompt.
    """
    lines = ["Available tables and their columns:\n"]

    for table_name, table_info in SCHEMA["tables"].items():
        table_type = table_info["type"]
        lines.append(f"Table: {table_name} ({table_type})")
        for col_name, col_type in table_info["columns"].items():
            lines.append(f"  - {col_name} ({col_type})")
        lines.append("")

    lines.append("Relationships:")
    for rel in SCHEMA["relationships"]:
        lines.append(
            f"  - {rel['from_table']}.{rel['from_column']} "
            f"-> {rel['to_table']}.{rel['to_column']}"
        )

    return "\n".join(lines)


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(get_schema_for_llm())