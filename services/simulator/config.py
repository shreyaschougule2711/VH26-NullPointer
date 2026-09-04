import json
import os
from typing import Dict, Any

DEFAULT_PROFILES_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "config", "traffic_profiles.json")


def load_traffic_profiles() -> Dict[str, Any]:
    if os.path.exists(DEFAULT_PROFILES_PATH):
        try:
            with open(DEFAULT_PROFILES_PATH, "r") as f:
                return json.load(f).get("scenarios", {})
        except Exception:
            pass
    return {
        "NORMAL": {
            "baseEps": 50,
            "spikeMultiplier": 1.0,
            "rampUpSeconds": 0,
            "peakDurationSeconds": 300,
            "rampDownSeconds": 0,
            "eventDistribution": {
                "PAYMENT": 0.15,
                "ORDER": 0.20,
                "REFUND": 0.05,
                "INVENTORY": 0.20,
                "NOTIFICATION": 0.15,
                "CLICK": 0.15,
                "LOG": 0.10
            }
        }
    }
