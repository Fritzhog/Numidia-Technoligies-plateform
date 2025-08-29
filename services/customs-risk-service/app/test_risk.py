from app.main import compute_risk, Declaration


def test_risk_levels():
    low = Declaration(nin="X", goods="textile", value=1000, origin_country="DZ")
    assert compute_risk(low)['level'] == "LOW"
    med = Declaration(nin="X", goods="textile", value=120000, origin_country="DZ")
    assert compute_risk(med)['level'] == "MEDIUM"
    high = Declaration(nin="X", goods="electronic components", value=120000, origin_country="KY")
    assert compute_risk(high)['level'] == "HIGH"
