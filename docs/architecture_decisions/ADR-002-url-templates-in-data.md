# ADR-002: URL Templates as Data

Dealer inventory URL formats differ by CMS, model slug rules, and SmartPath vs standard paths. We store templates as data (CSV/YAML with placeholders) and generate URLs via a single builder function. Benefits: zero hard-coded branches, easy overrides per dealer, snapshot tests to prevent regressions.
