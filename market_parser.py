"""
market_parser.py — Parse Polymarket market questions into structured data.

Extracts:
  A. Strike markets: "Bitcoin above 70,400 on March 26, 1AM ET"
     → strike_price=70400, comparison_type="above"
  B. Up/Down markets: "Bitcoin Up or Down - March 26, 12:30AM-12:45AM ET"
     → market_type="up_down", start_time, end_time

Integrated into discovery — called when new markets are found.
"""

import re
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# Regex for strike markets: "Bitcoin above 70,400 on March 26, 1AM ET"
STRIKE_RE = re.compile(
    r"bitcoin\s+(above|below)\s+([\d,]+(?:\.\d+)?)\s+on\s+(.+?)(?:\?|$)",
    re.IGNORECASE,
)

# Regex for up/down markets: "Bitcoin Up or Down - March 26, 12:30AM-12:45AM ET"
UPDOWN_RE = re.compile(
    r"bitcoin\s+up\s+or\s+down\s*[-–—]\s*(.+?)(?:\?|$)",
    re.IGNORECASE,
)

# Time patterns within the date string
TIME_RANGE_RE = re.compile(
    r"(\d{1,2}(?::\d{2})?\s*[AP]M)\s*[-–—]\s*(\d{1,2}(?::\d{2})?\s*[AP]M)\s*(ET|EST|EDT|UTC)?",
    re.IGNORECASE,
)

SINGLE_TIME_RE = re.compile(
    r"(\d{1,2}(?::\d{2})?\s*[AP]M)\s*(ET|EST|EDT|UTC)?",
    re.IGNORECASE,
)

# Month mapping
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9,
    "oct": 10, "nov": 11, "dec": 12,
}

# ET offset (EST = UTC-5, EDT = UTC-4) — use EDT as default for US markets
ET_OFFSET = timedelta(hours=-4)


def _parse_number(s):
    """Parse '70,400' or '70400.5' into float."""
    return float(s.replace(",", ""))


def _parse_datetime_str(date_part, time_str, year=None):
    """
    Parse 'March 26' + '1AM' into a UTC datetime.
    Assumes ET timezone.
    """
    if year is None:
        year = datetime.now(timezone.utc).year

    # Extract month and day from date_part
    date_part = date_part.strip().rstrip(",")
    month = None
    day = None
    for m_name, m_num in MONTHS.items():
        if m_name in date_part.lower():
            month = m_num
            # Find day number after month name
            remainder = date_part.lower().split(m_name)[-1].strip()
            day_match = re.search(r"(\d{1,2})", remainder)
            if day_match:
                day = int(day_match.group(1))
            break

    if month is None or day is None:
        return None

    # Parse time like "1AM", "12:30PM", "1:00AM"
    time_str = time_str.strip()
    time_match = re.match(r"(\d{1,2})(?::(\d{2}))?\s*([AP]M)", time_str, re.IGNORECASE)
    if not time_match:
        return None

    hour = int(time_match.group(1))
    minute = int(time_match.group(2) or 0)
    ampm = time_match.group(3).upper()

    if ampm == "PM" and hour != 12:
        hour += 12
    elif ampm == "AM" and hour == 12:
        hour = 0

    # Build ET datetime, then convert to UTC
    local_dt = datetime(year, month, day, hour, minute, tzinfo=timezone(ET_OFFSET))
    return local_dt.astimezone(timezone.utc)


def parse_market_question(question, end_date_str=None):
    """
    Parse a market question string into structured data.

    Returns dict:
      market_type: "strike" or "up_down"
      strike_price: float or None
      comparison_type: "above" or "below" or None
      parsed_start_time: datetime or None
      parsed_end_time: datetime or None
    """
    result = {
        "market_type": None,
        "strike_price": None,
        "comparison_type": None,
        "parsed_start_time": None,
        "parsed_end_time": None,
    }

    # Try strike pattern first
    strike_match = STRIKE_RE.search(question)
    if strike_match:
        result["market_type"] = "strike"
        result["comparison_type"] = strike_match.group(1).lower()
        result["strike_price"] = _parse_number(strike_match.group(2))

        date_time_str = strike_match.group(3).strip()
        # Extract the single time from the date string
        time_match = SINGLE_TIME_RE.search(date_time_str)
        if time_match:
            time_str = time_match.group(1)
            # Get date part (everything before the time)
            date_part = date_time_str[:time_match.start()].strip().rstrip(",")
            parsed = _parse_datetime_str(date_part, time_str)
            result["parsed_end_time"] = parsed

        return result

    # Try up/down pattern
    updown_match = UPDOWN_RE.search(question)
    if updown_match:
        result["market_type"] = "up_down"
        remainder = updown_match.group(1).strip()

        # Try to find time range
        time_range = TIME_RANGE_RE.search(remainder)
        if time_range:
            start_time_str = time_range.group(1)
            end_time_str = time_range.group(2)
            # Get date part
            date_part = remainder[:time_range.start()].strip().rstrip(",")
            result["parsed_start_time"] = _parse_datetime_str(date_part, start_time_str)
            result["parsed_end_time"] = _parse_datetime_str(date_part, end_time_str)
        else:
            # Single time (e.g., "Bitcoin Up or Down - March 26, 12AM ET")
            time_match = SINGLE_TIME_RE.search(remainder)
            if time_match:
                time_str = time_match.group(1)
                date_part = remainder[:time_match.start()].strip().rstrip(",")
                result["parsed_end_time"] = _parse_datetime_str(date_part, time_str)

        return result

    logger.warning("Could not parse market question: %s", question)
    return result


# SQL to add parsed columns to tracked_markets
ALTER_TRACKED_MARKETS_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='tracked_markets' AND column_name='market_type') THEN
        ALTER TABLE tracked_markets ADD COLUMN market_type TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='tracked_markets' AND column_name='strike_price') THEN
        ALTER TABLE tracked_markets ADD COLUMN strike_price DOUBLE PRECISION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='tracked_markets' AND column_name='comparison_type') THEN
        ALTER TABLE tracked_markets ADD COLUMN comparison_type TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='tracked_markets' AND column_name='parsed_start_time') THEN
        ALTER TABLE tracked_markets ADD COLUMN parsed_start_time TIMESTAMPTZ;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='tracked_markets' AND column_name='parsed_end_time') THEN
        ALTER TABLE tracked_markets ADD COLUMN parsed_end_time TIMESTAMPTZ;
    END IF;
END
$$;
"""


def ensure_parsed_columns(conn):
    """Add parsed columns to tracked_markets if they don't exist."""
    with conn.cursor() as cur:
        cur.execute(ALTER_TRACKED_MARKETS_SQL)
    conn.commit()


def parse_and_store(conn, market_id, question):
    """Parse a market question and update tracked_markets with parsed fields."""
    parsed = parse_market_question(question)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tracked_markets
            SET market_type = %s,
                strike_price = %s,
                comparison_type = %s,
                parsed_start_time = %s,
                parsed_end_time = %s
            WHERE market_id = %s
            """,
            (
                parsed["market_type"],
                parsed["strike_price"],
                parsed["comparison_type"],
                parsed["parsed_start_time"],
                parsed["parsed_end_time"],
                market_id,
            ),
        )
    conn.commit()

    logger.info(
        "Parsed market %s: type=%s strike=%s comp=%s",
        market_id, parsed["market_type"], parsed["strike_price"], parsed["comparison_type"],
    )
    return parsed


def backfill_unparsed(conn):
    """Parse all tracked markets that haven't been parsed yet."""
    ensure_parsed_columns(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT market_id, question FROM tracked_markets WHERE market_type IS NULL")
        unparsed = cur.fetchall()

    count = 0
    for market_id, question in unparsed:
        if question:
            parse_and_store(conn, market_id, question)
            count += 1

    logger.info("Backfill parsed %d markets", count)
    return count


if __name__ == "__main__":
    # Test parsing
    tests = [
        "Bitcoin above 70,400 on March 26, 1AM ET?",
        "Bitcoin above 68,800 on March 26, 1AM ET?",
        "Bitcoin Up or Down - March 26, 12:30AM-12:45AM ET",
        "Bitcoin Up or Down - March 26, 12AM ET",
        "Bitcoin below 65,000 on March 26, 2PM ET?",
    ]
    for q in tests:
        result = parse_market_question(q)
        print(f"\n{q}")
        print(f"  type={result['market_type']}, strike={result['strike_price']}, "
              f"comp={result['comparison_type']}")
        print(f"  start={result['parsed_start_time']}, end={result['parsed_end_time']}")
