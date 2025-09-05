from django.apps import apps
from django.core.management import BaseCommand
from django.core.management import CommandError

from eventsourcing.aggregates import rebuild_aggregates
from eventsourcing.models import AggregateModel


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("model", type=str, help="The model to rebuild in 'app_label.Model' format.")
        parser.add_argument("--id", type=str, help="(Optional) The specific model ID to rebuild.")

    def handle(self, *args, **options):
        model = options["model"]
        id = options.get("id")
        app_label, model_name = model.split(".")
        model_class = apps.get_model(app_label, model_name)

        if not issubclass(model_class, AggregateModel):
            raise CommandError(f"{model} is not an AggregateModel.")

        total = 0
        chunk_size = 1000

        def prebuild_cb(t):
            self.stdout.write(f"Rebuilding {t} instances of {model}...")
            nonlocal total
            total = t

        def chunk_cb(i):
            self.stdout.write(f"Rebuilt chunk #{i} of {total // chunk_size + 1}.")

        rebuild_aggregates(
            model_class, chunk_size=chunk_size, prebuild_callback=prebuild_cb, chunk_callback=chunk_cb, model_id=id
        )

        self.stdout.write("Rebuild complete.")
