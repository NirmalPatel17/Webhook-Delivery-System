from app.main import allow_request

def test_rate_limit_exceeded():
    test_ip = "192.168.34.56"
    
    # First 3 requests should pass
    assert allow_request(test_ip) is True
    assert allow_request(test_ip) is True
    assert allow_request(test_ip) is True
    
    # request should be blocked if rate limit is 3/second
    assert allow_request(test_ip) is False
