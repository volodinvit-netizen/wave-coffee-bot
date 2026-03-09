def extract_poster_time(transaction: dict) -> datetime | None:
    keys = ["date_close", "date", "created_at", "closed_at", "time", "open_date", "close_date"]

    print("POSTER TIME RAW TRANSACTION:", transaction)

    for k in keys:
        v = transaction.get(k)
        print(f"POSTER TIME CANDIDATE {k} =", repr(v))

        if not v:
            continue

        # unix timestamp in seconds
        if isinstance(v, (int, float)) and 1000000000 <= float(v) < 1000000000000:
            try:
                dt = datetime.fromtimestamp(float(v), tz=timezone.utc)
                print(f"PARSED {k} AS UNIX SECONDS ->", dt.isoformat())
                return dt
            except Exception as e:
                print(f"FAILED PARSE {k} AS UNIX SECONDS:", e)

        # unix timestamp in milliseconds
        if isinstance(v, (int, float)) and float(v) >= 1000000000000:
            try:
                dt = datetime.fromtimestamp(float(v) / 1000, tz=timezone.utc)
                print(f"PARSED {k} AS UNIX MILLISECONDS ->", dt.isoformat())
                return dt
            except Exception as e:
                print(f"FAILED PARSE {k} AS UNIX MILLISECONDS:", e)

        if isinstance(v, str):
            s = v.strip()

            # ISO with Z
            if s.endswith("Z"):
                try:
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
                    print(f"PARSED {k} AS ISO Z ->", dt.isoformat())
                    return dt
                except Exception as e:
                    print(f"FAILED PARSE {k} AS ISO Z:", e)

            # ISO with timezone
            try:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=LOCAL_TZ)
                dt = dt.astimezone(timezone.utc)
                print(f"PARSED {k} AS ISO ->", dt.isoformat())
                return dt
            except Exception:
                pass

            # common formats without timezone -> assume local time of Akтау
            fmts = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%d.%m.%Y %H:%M:%S",
                "%d.%m.%Y %H:%M",
                "%Y-%m-%d %H:%M:%S %z",
                "%Y-%m-%d %H:%M %z",
            ]
            for fmt in fmts:
                try:
                    dt = datetime.strptime(s, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=LOCAL_TZ)
                    dt = dt.astimezone(timezone.utc)
                    print(f"PARSED {k} WITH FORMAT {fmt} ->", dt.isoformat())
                    return dt
                except Exception:
                    continue

    print("POSTER TIME PARSE FAILED: no usable datetime found")
    return None
