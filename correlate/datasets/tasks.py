from celery import shared_task
import time
from datasets.lib.report import generate_automatic_report


@shared_task
def add(x, y):
    time.sleep(5)
    return x + y


@shared_task
def generate_automatic_report_task(stock: str, user_id: int):
    generate_automatic_report(stock, user_id)
