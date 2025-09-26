# pdfmap_project/management/commands/run_asgi.py
from django.core.management.base import BaseCommand
from daphne.cli import CommandLineInterface


class Command(BaseCommand):
    help = 'Run ASGI server with Daphne on port 6001'

    def add_arguments(self, parser):
        parser.add_argument(
            '--host',
            default='0.0.0.0',
            help='Host to bind to (default: 0.0.0.0)'
        )
        parser.add_argument(
            '--port',
            type=int,
            default=6001,
            help='Port to bind to (default: 6001)'
        )
        parser.add_argument(
            '--access-log',
            action='store_true',
            help='Enable access logging'
        )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS(f'Starting ASGI server on {options["host"]}:{options["port"]}')
        )

        # Build Daphne command arguments
        daphne_args = [
            '--bind', options['host'],
            '--port', str(options['port']),
            'pdfmap_project.asgi:application'
        ]

        if options['access_log']:
            daphne_args.insert(-1, '--access-log')

        # Run Daphne
        CommandLineInterface().run(daphne_args)


