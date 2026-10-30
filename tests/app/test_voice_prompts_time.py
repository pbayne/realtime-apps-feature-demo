"""The assistant prompt names weekdays so relative dates resolve correctly (a live run put 'next Tuesday' on a Wednesday)."""

from datetime import datetime, timezone

from voice.prompts import time_context


def test_time_context_lists_the_next_seven_days_by_name():
    text = time_context(datetime(2026, 10, 30, 17, 0, tzinfo=timezone.utc))   # a Friday
    assert "Friday 2026-10-30T17:00Z" in text
    assert "Tuesday 2026-11-03" in text and "Wednesday 2026-11-04" in text
    assert "Friday 2026-10-30," not in text.split("next seven days:")[1]   # today is not in the list
