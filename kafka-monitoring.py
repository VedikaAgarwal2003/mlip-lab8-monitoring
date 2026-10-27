"""Lab monitor: --check validates Kafka; --demo generates synthetic events."""
import argparse
import math
import random
import re
import time
import uuid
import warnings


def parse_event(event):
    fields = event.split(',')
    if len(fields) < 3 or not fields[2].strip().startswith('recommendation request '):
        return None
    if len(fields) < 5:
        raise ValueError('missing recommendation fields')
    match = re.fullmatch(r'status (\d{1,3})', fields[3].strip())
    if not match:
        raise ValueError('invalid status field')
    latency = re.fullmatch(r'(\d+(?:\.\d+)?)\s+ms', fields[-1].strip())
    if not latency:
        raise ValueError('expected final latency field in milliseconds, e.g. 87 ms')
    seconds = float(latency[1]) / 1000
    if not math.isfinite(seconds):
        raise ValueError('non-finite latency')
    return match[1], seconds


def live_events(args):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        args.topic, bootstrap_servers=args.bootstrap,
        group_id='lab08-' + uuid.uuid4().hex,
        auto_offset_reset='latest', enable_auto_commit=False,
        consumer_timeout_ms=30000 if args.check else float('inf'),
    )
    deadline = time.monotonic() + 30 if args.check else float('inf')
    try:
        for message in consumer:
            if time.monotonic() >= deadline:
                return
            try:
                parsed = parse_event(message.value.decode('utf-8'))
            except (ValueError, UnicodeDecodeError) as exc:
                warnings.warn('Skipping malformed recommendation event: ' + str(exc))
                continue
            if parsed is not None:
                yield parsed
    finally:
        consumer.close()


def demo_events():
    rng = random.Random(8)
    while True:
        yield ('200' if rng.random() < 0.9 else '500', rng.uniform(0.01, 0.3))
        time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--topic')
    parser.add_argument('--bootstrap', default='localhost:9092')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--check', action='store_true')
    mode.add_argument('--demo', action='store_true')
    args = parser.parse_args()
    if not args.demo and not args.topic:
        parser.error('--topic is required unless --demo is used')
    if args.check:
        event = next(live_events(args), None)
        if event is None:
            raise SystemExit('No valid recommendation event in the check window; ask the TA or use --demo.')
        print('Kafka check passed: status=%s, latency=%s seconds' % event)
        return

    from prometheus_client import Counter, Histogram, start_http_server
    # TODO 1: Replace None with a Counter using name request_count and label http_status.
    REQUEST_COUNT = None
    if REQUEST_COUNT is None:
        raise SystemExit('Complete the request counter definition TODO first.')
    REQUEST_LATENCY = Histogram(
        'request_latency_seconds', 'Recommendation latency in seconds',
        buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10),
    )
    start_http_server(8765, addr='0.0.0.0')
    print('Mode: ' + ('SYNTHETIC DATA' if args.demo else 'live Kafka'), flush=True)
    first = True
    for status, seconds in (demo_events() if args.demo else live_events(args)):
        # TODO 2: Replace this raise with REQUEST_COUNT.labels(...).inc().
        raise NotImplementedError('Increment the counter for status before observing latency.')
        REQUEST_LATENCY.observe(seconds)
        if first:
            print('First observation: status=%s, latency=%s seconds' % (status, seconds), flush=True)
            first = False


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
