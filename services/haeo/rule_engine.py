import json
import os
from typing import Dict, Any
from services.common.models import EventType, DecisionOutcome
from services.common.logger import get_logger

logger = get_logger("RuleEngine")

RULES_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "config", "haeo_rules.json")


class RuleEngine:
    def __init__(self):
        self.rules = self._load_rules()

    def _load_rules(self) -> Dict[str, Any]:
        if os.path.exists(RULES_FILE_PATH):
            try:
                with open(RULES_FILE_PATH, "r") as f:
                    return json.load(f).get("eventSlaRules", {})
            except Exception as e:
                logger.error(f"Error reading haeo_rules.json: {e}")
        return {}

    def get_event_sla(self, event_type: EventType) -> Dict[str, Any]:
        key = event_type.value
        return self.rules.get(key, {
            "basePriority": 10,
            "canDrop": True,
            "canBatch": True,
            "maxDelayMs": 5000,
            "defaultTopic": "deferred-events"
        })

    def validate_decision(self, event_type: EventType, proposed_decision: DecisionOutcome) -> DecisionOutcome:
        sla = self.get_event_sla(event_type)

        # Rule 1: PAYMENT & ORDER & REFUND must NEVER be dropped
        if proposed_decision == DecisionOutcome.SHED:
            if not sla.get("canDrop", True):
                logger.warning(f"Prevented dropping non-droppable event type {event_type}. Overriding to PROCESS.")
                return DecisionOutcome.PROCESS

        # Rule 2: PAYMENT & ORDER & REFUND cannot be batched
        if proposed_decision == DecisionOutcome.BATCH:
            if not sla.get("canBatch", True):
                return DecisionOutcome.PROCESS

        return proposed_decision
