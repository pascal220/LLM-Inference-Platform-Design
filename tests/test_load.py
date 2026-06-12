"""
Locust load test for the LLM Inference Platform.

Run with:
    locust -f tests/load_test.py --host=http://localhost:8000

Then open http://localhost:8089 to configure and start the test.
"""

import json
from locust import HttpUser, task, between, events


PREMIUM_KEY = "key-acme-premium"
STANDARD_KEY = "key-beta-standard"

SAMPLE_MESSAGES = [
    [{"role": "user", "content": "What is the capital of France?"}],
    [{"role": "user", "content": "Explain quantum computing in simple terms."}],
    [{"role": "user", "content": "Write a haiku about autumn leaves."}],
    [{"role": "user", "content": "What are the benefits of exercise?"}],
]


class PremiumUser(HttpUser):
    """
    Simulates a premium-tier tenant.
    Higher request rate, expects lower latency.
    """
    wait_time = between(0.5, 1.5)
    weight = 3  # 3x more premium users in the mix

    @task
    def chat_streaming(self):
        messages = SAMPLE_MESSAGES[
            hash(self.environment.runner.user_count) % len(SAMPLE_MESSAGES)
        ]
        payload = {"messages": messages, "max_tokens": 100}

        tokens_received = 0
        first_token_time = None
        start_time = self.environment.runner.stats.total.start_time

        with self.client.post(
            "/v1/chat",
            json=payload,
            headers={
                "Authorization": f"Bearer {PREMIUM_KEY}",
                "Accept": "text/event-stream",
            },
            stream=True,
            catch_response=True,
            name="/v1/chat [premium]",
        ) as response:
            if response.status_code != 200:
                response.failure(
                    f"Expected 200, got {response.status_code}"
                )
                return

            for line in response.iter_lines():
                if line:
                    decoded = line.decode("utf-8") if isinstance(line, bytes) else line
                    if decoded.startswith("data:"):
                        data = decoded[5:].strip()
                        if data == "[DONE]":
                            response.success()
                            break
                        try:
                            parsed = json.loads(data)
                            if "token" in parsed:
                                tokens_received += 1
                        except json.JSONDecodeError:
                            pass


class StandardUser(HttpUser):
    """
    Simulates a standard-tier tenant.
    Lower request rate, more tolerant of latency.
    """
    wait_time = between(2.0, 5.0)
    weight = 1

    @task
    def chat_streaming(self):
        messages = [{"role": "user", "content": "Tell me a short joke."}]
        payload = {"messages": messages, "max_tokens": 50}

        with self.client.post(
            "/v1/chat",
            json=payload,
            headers={
                "Authorization": f"Bearer {STANDARD_KEY}",
                "Accept": "text/event-stream",
            },
            stream=True,
            catch_response=True,
            name="/v1/chat [standard]",
        ) as response:
            if response.status_code == 429:
                # Rate limited — expected under load, not a failure
                response.success()
                return
            if response.status_code != 200:
                response.failure(
                    f"Expected 200, got {response.status_code}"
                )
                return

            for line in response.iter_lines():
                if line:
                    decoded = line.decode("utf-8") if isinstance(line, bytes) else line
                    if decoded.startswith("data:") and "[DONE]" in decoded:
                        response.success()
                        break


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("\n🚀 Load test starting...")
    print(f"   Target: {environment.host}")
    print(f"   Premium key: {PREMIUM_KEY}")
    print(f"   Standard key: {STANDARD_KEY}\n")