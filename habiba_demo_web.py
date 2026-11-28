"""Flask routes for the isolated HABIBA lifecycle demo preview."""
from __future__ import annotations

from pathlib import Path
from flask import Blueprint, jsonify, request, send_from_directory

from habiba_demo import DEMO_ENGINE


demo_blueprint = Blueprint("habiba_lifecycle_demo", __name__, url_prefix="/lifecycle")
_STATIC_DIR = Path(__file__).resolve().parent / "static"


@demo_blueprint.get("/")
def lifecycle_demo_page():
    response = send_from_directory(_STATIC_DIR, "habiba_lifecycle_demo.html", max_age=0)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["X-Robots-Tag"] = "noindex, nofollow"
    return response


@demo_blueprint.get("/api/state")
def lifecycle_demo_state():
    response = jsonify(DEMO_ENGINE.snapshot())
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response, 200


@demo_blueprint.post("/api/control")
def lifecycle_demo_control():
    payload = request.get_json(silent=True) or {}
    action = payload.get("action")
    try:
        state = DEMO_ENGINE.control(action)
        response = jsonify(state)
        response.headers["Cache-Control"] = "no-store, max-age=0"
        return response, 200
    except ValueError as exc:
        return jsonify({"error": str(exc), "mode": "DEMO_SIMULATION_ONLY"}), 400
    except Exception as exc:  # defensive API boundary; never involve live trading handlers
        return jsonify({"error": f"demo_control_failed:{type(exc).__name__}", "mode": "DEMO_SIMULATION_ONLY"}), 500
