import pandas as pd
import numpy as np

def _to_num(x):
    # helper to convert numpy types to native python
    if isinstance(x, (np.floating, np.float32, np.float64)):
        return float(x)
    if isinstance(x, (np.integer, np.int32, np.int64)):
        return int(x)
    return x

def monte_carlo_projection(fund_id, initial=100000, years_list=[1,5,10], n_sims=3000, steps_per_year=1):
    df = pd.read_csv("fund_allocation.csv")
    group = df[df["FUND_ID"] == fund_id]
    if group.empty:
        raise ValueError("Fund ID not found in fund_allocation.csv")

    fund_name = str(group["FUND_NAME"].iloc[0])

    weights = group["PERCENT_OF_FUND"].astype(float) / 100.0
    mu = float(np.sum(weights * group["AVG_RETURN_10Y"].astype(float)) / 100.0)
    sigma = float(np.sum(weights * group["STD_DEV_10Y"].astype(float)) / 100.0)

    results = []
    for horizon in years_list:
        steps = int(horizon * steps_per_year)
        dt = 1.0 / steps_per_year

        # vectorized simulation
        rand = np.random.normal(0, 1, size=(n_sims, steps))
        drift = (mu - 0.5 * sigma**2) * dt
        diffusion = sigma * np.sqrt(dt) * rand
        log_returns = drift + diffusion
        log_paths = np.cumsum(log_returns, axis=1)
        log_paths = np.hstack((np.zeros((n_sims, 1)), log_paths))
        paths = initial * np.exp(log_paths)  # shape (n_sims, steps+1)

        percentiles = np.percentile(paths, [10, 25, 50, 75, 90], axis=0)

        timeline = list(range(0, steps + 1))
        p10 = [ _to_num(round(x, 0)) for x in percentiles[0].tolist() ]
        p25 = [ _to_num(round(x, 0)) for x in percentiles[1].tolist() ]
        p50 = [ _to_num(round(x, 0)) for x in percentiles[2].tolist() ]
        p75 = [ _to_num(round(x, 0)) for x in percentiles[3].tolist() ]
        p90 = [ _to_num(round(x, 0)) for x in percentiles[4].tolist() ]

        results.append({
            "years": int(horizon),
            "timeline": timeline,
            "p10": p10,
            "p25": p25,
            "median": p50,
            "p75": p75,
            "p90": p90,
            "final_p25": _to_num(p25[-1]),
            "final_median": _to_num(p50[-1]),
            "final_p75": _to_num(p75[-1]),
            # Do NOT include enormous arrays in JSON unless you need them on client.
            # If you still want sims for any reason, only include final values or a small sampled array.
        })

    return {"fund_name": fund_name, "results": results}
