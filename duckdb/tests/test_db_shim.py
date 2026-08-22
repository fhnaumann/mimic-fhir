"""Exercise py_mimic_fhir/db.py's DUCKDB branch against a real database file.

The HAPI validation path queries `mimic_fhir.*` and `fhir_trm.*` directly rather
than reading ndjson (HANDOFF.md 2), so the port needs this branch to behave like
the POSTGRES one. The two places it can silently differ:

  * `fhir` arrives as a str from DuckDB and as a dict from psycopg2, and every
    caller indexes it (`fhir['id']`). A str would raise TypeError: string indices
    must be integers -- but only inside a worker process, where validate.py
    catches broadly and logs.
  * validate.py builds a connection in the parent and then forks workers. An
    inherited DuckDB handle is not safe to use.

Run:  python duckdb/tests/test_db_shim.py
"""
import importlib.util
import json
import os
import pathlib
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parents[2]


def load_db_module():
    """Import py_mimic_fhir/db.py by path.

    Not `from py_mimic_fhir import db`: this test must not depend on the rest of
    the package, and db.py is deliberately importable without psycopg2 /
    pandas_gbq / google.auth installed. Importing it plainly is itself part of
    what is being checked.
    """
    spec = importlib.util.spec_from_file_location(
        "mf_db_under_test", REPO / "py_mimic_fhir" / "db.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def build_fixture(path):
    """A miniature of what driver.py --materialize writes."""
    import duckdb
    con = duckdb.connect(path)
    con.execute("CREATE SCHEMA mimic_fhir; CREATE SCHEMA fhir_trm;")

    def rows(recs):
        return ", ".join(
            "({}, {}, {})".format(
                _lit(r["id"]), _lit(r["patient_id"]),
                _lit(json.dumps(r["fhir"])))
            for r in recs)

    pats = [
        {"id": "pat-1", "patient_id": "pat-1",
         "fhir": {"resourceType": "Patient", "id": "pat-1"}},
        {"id": "pat-2", "patient_id": "pat-2",
         "fhir": {"resourceType": "Patient", "id": "pat-2"}},
    ]
    conds = [
        {"id": "con-1", "patient_id": "pat-1",
         "fhir": {"resourceType": "Condition", "id": "con-1",
                  # a '#' and a quote, because the ndjson writer had a bug with
                  # exactly these and JSON round-tripping is the thing under test
                  "code": {"text": 'Chest Tube #1 "left"'}}},
        {"id": "con-2", "patient_id": "pat-1",
         "fhir": {"resourceType": "Condition", "id": "con-2"}},
        {"id": "con-3", "patient_id": "pat-2",
         "fhir": {"resourceType": "Condition", "id": "con-3"}},
    ]
    for tbl, recs in (("patient", pats), ("condition", conds)):
        con.execute(
            f"CREATE TABLE mimic_fhir.{tbl} "
            f"(id VARCHAR, patient_id VARCHAR, fhir JSON)")
        con.execute(f"INSERT INTO mimic_fhir.{tbl} VALUES {rows(recs)}")

    con.execute("CREATE TABLE fhir_trm.cs_example (code VARCHAR, display VARCHAR)")
    con.execute("INSERT INTO fhir_trm.cs_example VALUES ('a', 'Alpha'), ('b', 'Beta')")
    con.close()


def _lit(s):
    return "'" + str(s).replace("'", "''") + "'"


CHECKS = []


def check(name):
    def deco(fn):
        CHECKS.append((name, fn))
        return fn
    return deco


@check("get_n_patient_id returns ids, not raw JSON strings")
def _(db, path):
    ids = db.get_n_patient_id()
    assert sorted(ids) == ["pat-1", "pat-2"], ids
    # This is the assertion that fails if the json.loads is dropped: indexing a
    # str with 'id' raises TypeError rather than returning anything.
    assert all(isinstance(i, str) for i in ids)


@check("get_n_patient_id honours the LIMIT")
def _(db, path):
    assert len(db.get_n_patient_id(1)) == 1


@check("get_resources_by_pat keys off id for patient, patient_id otherwise")
def _(db, path):
    pat = db.get_resources_by_pat("patient", "pat-1")
    assert len(pat) == 1 and pat[0]["id"] == "pat-1", pat
    con = db.get_resources_by_pat("condition", "pat-1")
    assert sorted(c["id"] for c in con) == ["con-1", "con-2"], con


@check("nested JSON survives the round trip verbatim")
def _(db, path):
    (c,) = [r for r in db.get_resources_by_pat("condition", "pat-1")
            if r["id"] == "con-1"]
    assert c["code"]["text"] == 'Chest Tube #1 "left"', c


@check("get_resource_by_id returns a dict")
def _(db, path):
    r = db.get_resource_by_id("condition", "con-3")
    assert isinstance(r, dict) and r["id"] == "con-3", r


@check("get_n_resources returns every row as a dict")
def _(db, path):
    rs = db.get_n_resources("condition")
    assert len(rs) == 3 and all(isinstance(r, dict) for r in rs), rs
    assert len(db.get_n_resources("condition", 2)) == 2


@check("get_table reads fhir_trm and leaves non-fhir columns alone")
def _(db, path):
    df = db.get_table("fhir_trm", "cs_example")
    assert len(df) == 2 and sorted(df["code"]) == ["a", "b"], df


@check("get_pat_id_with_links joins and returns an id")
def _(db, path):
    assert db.get_pat_id_with_links(["condition"]) in ("pat-1", "pat-2")


@check("the connection is read-only")
def _(db, path):
    try:
        db.db_conn.execute("CREATE TABLE mimic_fhir.nope (x INT)")
    except Exception:
        return
    raise AssertionError("a write succeeded on a read_only connection")


FORK_TIMEOUT = 25


def check_fork(mod, path):
    """A child forked from a parent that has already queried must not deadlock.

    This is the check that matters most, and it must be a TIMEOUT rather than an
    exception: the failure mode is a hang, not an error. An open DuckDB
    connection owns a background thread pool; fork() copies its mutexes without
    its threads, so the child blocks on its first query even if it opens a fresh
    connection of its own. Verified by construction -- with the before-fork close
    removed, this check hangs and is killed.

    `parent_ids` is fetched BEFORE the fork on purpose. That is what makes the
    parent hold a live handle, and it is exactly validate.py's shape: build a
    connection, call get_n_patient_id(), then fan out to workers.
    """
    import time
    if not hasattr(os, "fork"):
        return "skipped (no fork on this platform)"
    db = mod.MFDatabaseConnection(None, None, path, None, "DUCKDB")
    parent_ids = sorted(db.get_n_patient_id())

    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        code = 1
        try:
            got = sorted(db.get_n_patient_id())
            code = 0 if got == parent_ids else 2
            if db._duckdb_pid != os.getpid():
                code = 3
        except BaseException as e:  # noqa: BLE001
            os.write(w, f"{type(e).__name__}: {e}"[:300].encode())
            code = 4
        finally:
            os.close(w)
            os._exit(code)

    os.close(w)
    deadline = time.time() + FORK_TIMEOUT
    status = None
    while time.time() < deadline:
        done, st = os.waitpid(pid, os.WNOHANG)
        if done:
            status = st
            break
        time.sleep(0.1)
    if status is None:
        os.kill(pid, 9)
        os.waitpid(pid, 0)
        os.close(r)
        raise AssertionError(
            f"forked child still running after {FORK_TIMEOUT}s -- deadlocked on "
            "an inherited DuckDB thread pool; the before-fork close is missing "
            "or ineffective")
    msg = os.read(r, 300).decode()
    os.close(r)
    rc = os.waitstatus_to_exitcode(status)
    if rc != 0:
        raise AssertionError(f"forked child failed rc={rc} {msg}")

    # The parent must recover too: its handle was closed out from under it by the
    # before-fork hook, so this only works if reconnection is lazy.
    assert sorted(db.get_n_patient_id()) == parent_ids
    db.close()
    return None


def _pool_job(args):
    path, mod_path = args
    spec = importlib.util.spec_from_file_location("mf_db_worker", mod_path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    db = m.MFDatabaseConnection(None, None, path, None, "DUCKDB")
    out = sorted(db.get_n_patient_id())
    db.close()
    return out


def check_pool(mod, path):
    """The real caller's shape: validate.py's multiprocessing pool.

    check_fork covers the mechanism; this covers the actual API validate.py uses,
    including the default start method for the platform.
    """
    import multiprocessing as mp
    db = mod.MFDatabaseConnection(None, None, path, None, "DUCKDB")
    expected = sorted(db.get_n_patient_id())     # parent holds a handle, as validate.py does

    ctx = mp.get_context("fork") if "fork" in mp.get_all_start_methods() else mp.get_context()
    mod_path = str(REPO / "py_mimic_fhir" / "db.py")
    with ctx.Pool(2) as pool:
        res = pool.map_async(_pool_job, [(path, mod_path)] * 4)
        try:
            got = res.get(timeout=FORK_TIMEOUT)
        except mp.TimeoutError:
            pool.terminate()
            raise AssertionError(
                f"pool workers did not finish in {FORK_TIMEOUT}s -- this is the "
                "deadlock validate.py would hit")
    assert all(g == expected for g in got), got
    db.close()
    return f"start method: {ctx.get_start_method()}"


def main():
    mod = load_db_module()
    tmp = tempfile.mkdtemp(prefix="mf-db-shim-")
    path = os.path.join(tmp, "mimic.duckdb")
    build_fixture(path)

    failures = []
    db = mod.MFDatabaseConnection(None, None, path, None, "DUCKDB")
    for name, fn in CHECKS:
        try:
            fn(db, path)
            print(f"  ok    {name}")
        except Exception as e:  # noqa: BLE001
            failures.append((name, e))
            print(f"  FAIL  {name}\n          {type(e).__name__}: {e}")
    db.close()

    for name, fn in (("forked child does not deadlock", check_fork),
                     ("multiprocessing pool does not deadlock", check_pool)):
        try:
            note = fn(mod, path)
            print(f"  ok    {name}{' — ' + note if note else ''}")
        except Exception as e:  # noqa: BLE001
            failures.append((name, e))
            print(f"  FAIL  {name}\n          {type(e).__name__}: {e}")

    total = len(CHECKS) + 2
    if failures:
        print(f"\n{len(failures)} of {total} checks FAILED")
        return 1
    print(f"\n{total}/{total} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
