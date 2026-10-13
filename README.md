# Thread clothing data pipeline

Event-driven AWS data pipeline for a simulated clothing brand in Vietnam and Singapore: two warehouses, five stores, market websites, marketplace stores and POS.

The project prioritizes normalized, traceable data across orders, payments, inventory, returns, purchases and marketplace settlements.

- [Implementation and local commands](clothing-pipeline/README.md)
- [AWS deployment and cost controls](clothing-pipeline/DEPLOYMENT.md)
- [Design](clothing-data-pipeline-design-v1.md)

GitHub Actions runs offline tests, infrastructure validation, release builds and a history smoke test. AWS deployment is manually dispatched and defaults to creating a change set. The Glue schedule stays disabled on deployment.
