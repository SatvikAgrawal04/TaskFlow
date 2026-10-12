from __future__ import annotations

import json
from typing import Any


class DemoTimeline:
    def __init__(self, title: str) -> None:
        self.title = title
        self.step = 0

    def start(self) -> None:
        print()
        print(self.title)
        print("=" * len(self.title))

    def event(self, label: str, **fields: Any) -> None:
        self.step += 1
        print()
        print(f"[{self.step}] {label}")

        for key, value in fields.items():
            if isinstance(value, (dict, list)):
                value = json.dumps(value, sort_keys=True)
            print(f"    {key}: {value}")