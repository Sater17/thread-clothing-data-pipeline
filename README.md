# Thread clothing data pipeline

Event-driven AWS data pipeline for a simulated clothing brand in Vietnam and Singapore: two warehouses, five stores, market websites, marketplace stores and POS.

The project prioritizes normalized, traceable data across orders, payments, inventory, returns, purchases and marketplace settlements.

- [Implementation and local commands](clothing-pipeline/README.md)
- [AWS deployment and cost controls](clothing-pipeline/DEPLOYMENT.md)
- [AWS smoke validation](clothing-pipeline/AWS-VALIDATION.md)
- [Design](clothing-data-pipeline-design-v1.md)

V2 is under development with PySpark, Iceberg and failure email monitoring; it has not been deployed to AWS. The linked AWS validation covers V1 only. GitHub Actions runs unit/reference tests, Spark/Iceberg/DynamoDB integration tests, infrastructure validation and release builds. AWS deployment is manually dispatched and defaults to creating a change set. The Glue schedule stays disabled on deployment.
