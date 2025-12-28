import httpx

def test_downstream_random_failures():
    failures = 0
    for _ in range(20):
        r = httpx.post("http://downstream-service:8001/downstream/receive", json={})
        if r.status_code in (429, 500):
            failures += 1

    assert failures >= 2