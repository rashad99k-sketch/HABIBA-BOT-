"""Regression tests for foreground main-loop stall diagnostics."""
import threading
import unittest

from naiso_health import ComponentHealth, HealthWatchdog


class MainLoopStackDiagnosticsTests(unittest.TestCase):
    def test_main_thread_stack_is_captured_for_main_loop(self):
        watchdog = HealthWatchdog()
        component = ComponentHealth(
            name="main_loop",
            interval_sec=5.0,
            grace_sec=60.0,
            thread_name="MainThread",
        )

        snapshot = watchdog._stack_snapshot(component)
        self.assertTrue(snapshot["possible_stacks"])
        current_stack = snapshot["possible_stacks"].get(str(threading.get_ident()), "")
        self.assertIn("test_main_thread_stack_is_captured_for_main_loop", current_stack)

    def test_stack_excerpt_is_available_for_incident_payload(self):
        diagnostics = {
            "possible_stacks": {
                str(threading.get_ident()): "frame one\\nblocking_exchange_call\\nframe three"
            }
        }
        excerpt = HealthWatchdog._main_thread_stack_excerpt(diagnostics)
        self.assertIn("blocking_exchange_call", excerpt)
        self.assertLessEqual(len(excerpt), 2600)

    def test_missing_stack_is_reported_as_empty_not_fabricated(self):
        self.assertEqual(HealthWatchdog._main_thread_stack_excerpt({}), "")


if __name__ == "__main__":
    unittest.main()
