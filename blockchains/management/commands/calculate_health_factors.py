"""
Django management command to calculate health factor metrics for liquidations.

This command can be used to test and manually trigger the health factor calculation
task for liquidation events.
"""

from django.core.management.base import BaseCommand, CommandError

from blockchains.tasks import CalculateLiquidationHealthFactorMetricsTask


class Command(BaseCommand):
    help = "Calculate health factor metrics for liquidation events"

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=500,
            help="Number of recent liquidations to process (default: 500)",
        )
        parser.add_argument(
            "--async",
            action="store_true",
            help="Run the task asynchronously using Celery",
        )

    def handle(self, *args, **options):
        limit = options["limit"]
        async_mode = options["async"]

        self.stdout.write(
            self.style.SUCCESS(
                f"Starting health factor calculation for {limit} liquidations..."
            )
        )

        try:
            if async_mode:
                # Run asynchronously using Celery
                task_result = CalculateLiquidationHealthFactorMetricsTask.delay(
                    limit=limit
                )
                self.stdout.write(
                    self.style.SUCCESS(f"Task queued with ID: {task_result.id}")
                )
            else:
                # Run synchronously
                result = CalculateLiquidationHealthFactorMetricsTask.run(limit=limit)

                # Display results
                self.stdout.write(
                    self.style.SUCCESS(f"Task completed: {result['status']}")
                )

                if result["status"] == "completed":
                    total_liquidations = result.get("total_liquidations", "unknown")
                    self.stdout.write(
                        f"Processed: {result['processed_count']}/{total_liquidations} liquidations"
                    )

                    if result["errors"]:
                        self.stdout.write(
                            self.style.WARNING(
                                f"Errors encountered: {len(result['errors'])}"
                            )
                        )
                        for error in result["errors"][:5]:  # Show first 5 errors
                            self.stdout.write(f"  - {error}")
                        if len(result["errors"]) > 5:
                            self.stdout.write(
                                f"  ... and {len(result['errors']) - 5} more errors"
                            )
                elif result["status"] == "error":
                    self.stdout.write(
                        self.style.ERROR(f"Task failed: {result['error']}")
                    )
                elif result["status"] == "skipped":
                    self.stdout.write(
                        self.style.WARNING(f"Task skipped: {result['reason']}")
                    )

        except Exception as e:
            raise CommandError(f"Error executing task: {str(e)}")

        self.stdout.write(
            self.style.SUCCESS("Health factor calculation command completed.")
        )
