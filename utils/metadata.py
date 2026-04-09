# Centralized metadata for the Workplace Relations Commission (WRC) data pipeline.

BODIES = [
    {"id": "1", "name": "Employment Appeals Tribunal"},
    {"id": "2", "name": "Equality Tribunal"},
    {"id": "3", "name": "Labour Court"},
    {"id": "15376", "name": "Workplace Relations Commission"},
]

def get_body_name(body_id):
    """Helper to resolve body name from ID."""
    mapping = {b["id"]: b["name"] for b in BODIES}
    return mapping.get(str(body_id), "Workplace Relations")
