def suggest_gold_products() -> list[dict[str, str]]:
    return [
        {
            "table": "gold.ticket_volume_daily",
            "purpose": "Track support workload trends by day, status, priority, category, and team.",
        },
        {
            "table": "gold.resolution_sla_summary",
            "purpose": "Measure SLA breach rate and resolution-time distribution from valid Silver timestamps.",
        },
        {
            "table": "gold.category_breakdown",
            "purpose": "Show canonical support category distribution after deterministic and AI enrichment.",
        },
    ]
